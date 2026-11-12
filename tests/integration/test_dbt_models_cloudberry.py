"""Real dbt SQL on disposable Cloudberry; RUN_DBT_TESTS=1 and CLOUDBERRY_* required."""

import importlib.util
import json
import os
import subprocess
from contextlib import closing
from pathlib import Path
from uuid import uuid4

import psycopg2
import pytest
from psycopg2 import sql

ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_DBT_TESTS") != "1",
    reason="Set RUN_DBT_TESTS=1 for disposable Cloudberry dbt tests",
)
spec = importlib.util.spec_from_file_location(
    "dbt_publish", ROOT / "dbt/scripts/build.py"
)
build = importlib.util.module_from_spec(spec)
spec.loader.exec_module(build)


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory):
    settings = {
        "host": os.getenv("CLOUDBERRY_HOST", "127.0.0.1"),
        "port": os.getenv("CLOUDBERRY_PORT", "7000"),
        "user": os.getenv("CLOUDBERRY_USER", "gpadmin"),
        "password": os.environ["CLOUDBERRY_PASSWORD"],
    }
    name = "dbt_test_" + uuid4().hex
    directory = tmp_path_factory.mktemp("dbt")
    with closing(psycopg2.connect(**settings, dbname="postgres")) as admin:
        admin.autocommit = True
        with admin.cursor() as c:
            c.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        try:
            with closing(psycopg2.connect(**settings, dbname=name)) as conn:
                with conn, conn.cursor() as c:
                    c.execute((ROOT / "infra/cloudberry/sql/staging.sql").read_text())
                    c.execute(
                        (ROOT / "infra/cloudberry/sql/dbt_inputs.sql").read_text()
                    )
                env = dict(
                    os.environ,
                    CLOUDBERRY_DATABASE=name,
                    DBT_TARGET_PATH=str(directory / "target"),
                    DBT_LOG_PATH=str(directory / "logs"),
                    DBT_PROFILES_DIR=str(ROOT / "infra/dbt/profiles"),
                )
                subprocess.run(
                    [
                        str(ROOT / ".venv/bin/dbt"),
                        "compile",
                        "--project-dir",
                        str(ROOT / "dbt"),
                        "--vars",
                        '{snapshot_end_date: "2026-11-10"}',
                    ],
                    env=env,
                    check=True,
                    capture_output=True,
                    text=True,
                )
                manifest = json.loads((directory / "target/manifest.json").read_text())
                yield conn, manifest
        finally:
            with admin.cursor() as c:
                c.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(name)))


@pytest.fixture
def state(warehouse):
    conn, manifest = warehouse
    with conn, conn.cursor() as c:
        c.execute(
            "TRUNCATE stg.movies, stg.movie_genres, stg.movie_countries, stg.top250"
        )
        c.execute("""INSERT INTO stg.movies(id,batch_id,source_updated_at,ingested_at,name,year,budget_value,budget_currency,votes_kp,rating_kp,fees_world_value,fees_world_currency)
        VALUES (1,'a','2026-11-01 10:00Z','2026-11-01 12:00Z','One',2000,100,'USD',1000,7,200,'USD'),
        (1,'b','2026-11-03 10:00Z','2026-11-03 12:00Z','One',2000,120,'USD',1100,7.1,250,'USD'),
        (2,'a','2026-11-01 10:00Z','2026-11-01 12:00Z','Two',null,null,null,2000,8,null,null),
        (3,'a','2026-11-01 10:00Z','2026-11-04 12:00Z','Three',2000,999,'$',1000,7,200,'$')""")
        c.execute(
            "INSERT INTO stg.movie_genres VALUES (1,1,'Drama','drama','a'),(1,1,'Drama','drama','b')"
        )
    build.publish(conn, manifest)
    return conn, manifest


def rows(conn, query):
    with conn, conn.cursor() as c:
        c.execute(query)
        return c.fetchall()


def snapshot(conn, manifest):
    return {
        n["name"]: sorted(
            rows(conn, sql.SQL("SELECT * FROM {}").format(build.relation(n))), key=repr
        )
        for n in build.order_models(manifest)
    }


def execute(conn, query):
    with conn, conn.cursor() as c:
        c.execute(query)


def test_repeat_and_full_refresh_equivalence(state, capsys):
    conn, manifest = state
    before = snapshot(conn, manifest)
    capsys.readouterr()
    build.publish(conn, manifest)
    updates = [
        line for line in capsys.readouterr().out.splitlines() if "deleted=" in line
    ]
    assert len(updates) == 21
    assert all("deleted=0, inserted=0" in line for line in updates)
    assert snapshot(conn, manifest) == before
    build.publish(conn, manifest, full_refresh=True)
    assert snapshot(conn, manifest) == before
    assert rows(
        conn,
        "SELECT budget_total FROM mart.mart_budget_current_by_currency WHERE source_currency_value='\u0024'",
    ) == [(None,)]
    assert rows(
        conn,
        "SELECT count(*) FROM core.fact_movie_catalog_daily WHERE movie_id=3 AND snapshot_date_sk<20261104",
    ) == [(0,)]


def test_late_data_reorders_history_without_rolling_back_current(state):
    conn, manifest = state
    old_sk = rows(conn, "SELECT movie_sk FROM core.dim_movie ORDER BY movie_sk")
    execute(
        conn,
        """INSERT INTO stg.movies(id,batch_id,source_updated_at,ingested_at,name,year,budget_value,budget_currency,votes_kp,rating_kp)
    VALUES (1,'late','2026-11-02 10:00Z','2026-11-10 12:00Z','One',2000,110,'USD',1050,7.05)""",
    )
    build.publish(conn, manifest)
    assert [
        r[0]
        for r in rows(
            conn,
            "SELECT amount FROM core.fact_budget_history WHERE movie_id=1 ORDER BY valid_from",
        )
    ] == [100, 110, 120]
    assert rows(conn, "SELECT amount FROM core.v_budget_current WHERE movie_id=1") == [
        (120,)
    ]
    assert rows(conn, "SELECT movie_sk FROM core.dim_movie ORDER BY movie_sk") == old_sk
    expected = snapshot(conn, manifest)
    build.publish(conn, manifest, full_refresh=True)
    assert snapshot(conn, manifest) == expected


def test_conflicting_timestamp_rejects_entire_publication(state):
    conn, manifest = state
    before = snapshot(conn, manifest)
    execute(
        conn,
        "INSERT INTO stg.movies SELECT id,'conflict',source_page,source_updated_at,created_at,ingested_at,type,'Conflicting',alternative_name,year,movie_length,age_rating,rating_mpaa,status,is_series,tickets_on_sale,imdb_id,tmdb_id,kp_hd_id,rating_kp,rating_imdb,rating_tmdb,rating_film_critics,rating_russian_film_critics,votes_kp,votes_imdb,votes_tmdb,votes_film_critics,votes_russian_film_critics,top10,top250,budget_value,budget_currency,fees_world_value,fees_world_currency,fees_russia_value,fees_russia_currency,fees_usa_value,fees_usa_currency,premiere_world,premiere_russia,premiere_digital,premiere_dvd,premiere_bluray,genres_complete,countries_complete FROM stg.movies WHERE id=1 AND batch_id='a'",
    )
    with pytest.raises(RuntimeError, match="input_conflicts"):
        build.publish(conn, manifest)
    assert snapshot(conn, manifest) == before


def test_failure_after_write_rolls_back_all_models(state):
    conn, manifest = state
    before = snapshot(conn, manifest)
    execute(
        conn,
        "UPDATE stg.movies SET name='Changed',budget_value=333 WHERE id=1 AND batch_id='b'",
    )
    with pytest.raises(RuntimeError, match="Injected"):
        build.publish(conn, manifest, fail_after="fact_budget_history")
    assert snapshot(conn, manifest) == before


def test_removed_batch_rows_and_disappeared_groups_are_deleted(state):
    conn, manifest = state
    execute(
        conn,
        "DELETE FROM stg.movie_genres WHERE batch_id='b'; DELETE FROM stg.movies WHERE batch_id='b' OR id=3",
    )
    build.publish(conn, manifest)
    assert rows(conn, "SELECT amount FROM core.v_budget_current WHERE movie_id=1") == [
        (100,)
    ]
    assert rows(
        conn,
        "SELECT count(*) FROM mart.mart_budget_current_by_currency WHERE source_currency_value='$'",
    ) == [(0,)]
    expected = snapshot(conn, manifest)
    build.publish(conn, manifest, full_refresh=True)
    assert snapshot(conn, manifest) == expected


def test_unknown_collections_preserve_links_and_explicit_empty_removes(state):
    conn, manifest = state
    execute(conn, "DELETE FROM stg.movie_genres WHERE batch_id='b'")
    build.publish(conn, manifest)
    assert rows(conn, "SELECT count(*) FROM core.dim_movie WHERE movie_id=1") == [(1,)]
    execute(
        conn, "UPDATE stg.movies SET genres_complete=true WHERE id=1 AND batch_id='b'"
    )
    build.publish(conn, manifest)
    assert rows(conn, "SELECT count(*) FROM core.dim_movie WHERE movie_id=1") == [(2,)]
    assert rows(
        conn,
        "SELECT count(*) FROM core.bridge_movie_genre b JOIN core.v_movie_current m USING(movie_sk) WHERE m.movie_id=1",
    ) == [(0,)]


def test_null_transition_and_scd1_external_ids(state):
    conn, manifest = state
    execute(
        conn,
        "UPDATE stg.movies SET budget_value=NULL,budget_currency=NULL,imdb_id='tt-new' WHERE id=1 AND batch_id='b'",
    )
    build.publish(conn, manifest)
    assert rows(
        conn, "SELECT amount,currency_sk FROM core.v_budget_current WHERE movie_id=1"
    ) == [(None, 0)]
    assert rows(conn, "SELECT imdb_id FROM core.dim_movie WHERE movie_id=1") == [
        ("tt-new",)
    ]


def test_top250_complete_snapshots_and_movements(state):
    conn, manifest = state
    execute(
        conn,
        """INSERT INTO stg.top250
    SELECT '2026-11-01'::date,'top-a',i,i,'2026-11-01 12:00Z'::timestamptz FROM generate_series(1,250) i;
    INSERT INTO stg.top250
    SELECT '2026-11-02'::date,'top-b',i,i+1,'2026-11-02 12:00Z'::timestamptz FROM generate_series(1,250) i;
    INSERT INTO stg.top250
    SELECT '2026-11-02'::date,'partial',i,i+10,'2026-11-02 16:00Z'::timestamptz FROM generate_series(1,249) i""",
    )
    build.publish(conn, manifest)
    assert rows(conn, "SELECT count(*) FROM core.fact_top250_daily") == [(500,)]
    assert rows(
        conn, "SELECT movie_id FROM mart.mart_top250_daily_changes WHERE entered"
    ) == [(251,)]
    assert rows(
        conn, "SELECT movie_id FROM mart.mart_top250_daily_changes WHERE exited"
    ) == [(1,)]
    assert rows(
        conn,
        "SELECT position_change FROM mart.mart_top250_daily_changes WHERE movie_id=2",
    ) == [(1,)]

    before = snapshot(conn, manifest)
    execute(conn, "DELETE FROM stg.top250 WHERE batch_id='top-b' AND position=250")
    with pytest.raises(RuntimeError, match="Incomplete replacement"):
        build.publish(conn, manifest)
    assert snapshot(conn, manifest) == before


def test_weekly_delta_uses_boundary_states_and_keeps_negative_changes(state):
    conn, manifest = state
    execute(
        conn,
        """INSERT INTO stg.movies(id,batch_id,source_updated_at,ingested_at,name,year,votes_kp,rating_kp)
    VALUES (1,'week-start','2026-11-08 10:00Z','2026-11-08 12:00Z','One',2000,900,7.0),
    (1,'week-end','2026-11-09 10:00Z','2026-11-09 12:00Z','One',2000,850,6.9)""",
    )
    build.publish(conn, manifest)
    assert rows(
        conn,
        "SELECT votes_start,votes_end,votes_delta,is_complete_week FROM mart.mart_movie_popularity_weekly WHERE movie_id=1 AND week_start_date='2026-11-09'",
    ) == [(900, 850, -50, False)]
    assert rows(
        conn,
        "SELECT votes_start,votes_delta FROM mart.mart_movie_popularity_weekly WHERE movie_id=1 AND week_start_date='2026-10-26'",
    ) == [(None, None)]


def test_standard_dbt_incremental_strategy_on_postgres_adapter(state, tmp_path):
    conn, manifest = state
    before = snapshot(conn, manifest)
    env = dict(
        os.environ,
        CLOUDBERRY_DATABASE=conn.info.dbname,
        DBT_PROFILES_DIR=str(ROOT / "infra/dbt/profiles"),
        DBT_TARGET_PATH=str(tmp_path / "target"),
        DBT_LOG_PATH=str(tmp_path / "logs"),
    )
    result = subprocess.run(
        [
            str(ROOT / ".venv/bin/dbt"),
            "run",
            "--project-dir",
            str(ROOT / "dbt"),
            "--select",
            "fact_budget_history mart_budget_current_by_currency",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert snapshot(conn, manifest) == before
