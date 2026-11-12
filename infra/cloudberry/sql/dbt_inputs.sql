-- Apply before dbt. NULL completeness preserves the last known nonempty set.
ALTER TABLE stg.movies ADD COLUMN IF NOT EXISTS genres_complete boolean;
ALTER TABLE stg.movies ADD COLUMN IF NOT EXISTS countries_complete boolean;

CREATE TABLE IF NOT EXISTS stg.top250 (
    snapshot_date date NOT NULL,
    batch_id text NOT NULL,
    position integer NOT NULL CHECK (position BETWEEN 1 AND 250),
    movie_id bigint NOT NULL,
    observed_at timestamptz NOT NULL,
    PRIMARY KEY (snapshot_date, batch_id, position)
) DISTRIBUTED BY (snapshot_date);
