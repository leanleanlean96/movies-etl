"""Prepare the pool and activate daily only on its first registration."""

import argparse
import sys
import time

from airflow.models.dag import DagModel
from airflow.models.pool import Pool
from airflow.models.variable import Variable
from airflow.utils.session import create_session
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

DAG_ID = "kinopoisk_movies_daily"
POOL_NAME = "kinopoisk_processing"
STATE_KEY = "etl_pet_daily_deployment"


def record():
    # This runs after migration and before the DAG processor can register the DAG.
    with create_session() as session:
        state = session.scalar(select(Variable).where(Variable.key == STATE_KEY))
        if state is None:
            existing = session.get(DagModel, DAG_ID) is not None
            session.add(
                Variable(key=STATE_KEY, val="settled" if existing else "pending")
            )


def prepare_and_activate():
    with create_session() as session:
        state = session.scalar(
            select(Variable).where(Variable.key == STATE_KEY).with_for_update()
        )
        if state is None or state.val not in {"pending", "settled"}:
            raise RuntimeError(
                "Airflow deployment state is missing or invalid; check airflow-init."
            )

        if session.scalar(select(Pool.id).where(Pool.pool == POOL_NAME)) is None:
            session.add(
                Pool(
                    pool=POOL_NAME,
                    slots=1,
                    description="Serialize daily and replay processing",
                    include_deferred=False,
                )
            )

        if state.val == "settled":
            return True

        dag = session.get(DagModel, DAG_ID)
        if dag is None or dag.is_stale or dag.has_import_errors:
            return False

        # Commit activation and the marker together so a later pause survives retries.
        dag.is_paused = False
        state.val = "settled"
        return True


def activate():
    deadline = time.monotonic() + 300
    while time.monotonic() < deadline:
        if prepare_and_activate():
            print("Airflow processing pool and daily deployment are ready.")
            return
        time.sleep(2)
    raise RuntimeError(
        f"Timed out waiting for {DAG_ID}; check airflow-dag-processor logs."
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("record", "activate"))
    if parser.parse_args().phase == "record":
        record()
    else:
        activate()


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as error:
        sys.exit(str(error))
    except SQLAlchemyError as error:
        sys.exit(f"Airflow metadata initialization failed ({type(error).__name__}).")
