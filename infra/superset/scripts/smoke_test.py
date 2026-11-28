"""Check the versioned BI bundle on a running Superset after data is ready.

Set SUPERSET_URL (defaults to http://superset:8088), SUPERSET_ADMIN_PASSWORD,
and optionally SUPERSET_ADMIN_USERNAME and SUPERSET_ASSETS_DIR. The checks
authenticate and run queries through Superset without modifying the assets.
"""

import json
import os
import sys
from pathlib import Path

import requests
from bootstrap import Assets, BootstrapError, Superset, verify_import

QUERY_TIMEOUT = 60


def run_query(superset, context, label):
    result = superset.api(
        "POST",
        "chart/data",
        label,
        timeout=QUERY_TIMEOUT,
        json={**context, "force": True, "result_format": "json", "result_type": "full"},
    ).json()["result"]
    if len(result) != len(context["queries"]) or not all(
        query.get("status") == "success" and not query.get("error") for query in result
    ):
        raise BootstrapError(f"{label}: a query failed or results are missing.")
    return result


def check_chart_queries(superset, assets, inventory):
    for uuid, asset in assets.items["chart"].items():
        chart_id = inventory["chart", uuid]["id"]
        dataset_id = inventory["dataset", asset["dataset_uuid"]]["id"]
        label = f"Chart {chart_id} ({asset['slice_name']})"
        detail = superset.api("GET", f"chart/{chart_id}", f"{label} details").json()[
            "result"
        ]
        context = json.loads(detail.get("query_context") or "null")
        if not context or not context.get("queries"):
            raise BootstrapError(f"{label}: saved query context is missing.")
        if context["datasource"] != {"id": dataset_id, "type": "table"}:
            raise BootstrapError(f"{label}: saved query points to the wrong dataset.")
        result = run_query(superset, context, label)
        print(f"{label}: {', '.join(str(query['rowcount']) for query in result)} rows")


def check_filter_queries(superset, assets, inventory):
    for uuid, asset in assets.items["dashboard"].items():
        dashboard = inventory["dashboard", uuid]
        _, _, filters = superset.dashboard_graph(dashboard["id"])
        expected = {
            config["id"]: config["filterType"]
            for config in asset["metadata"]["native_filter_configuration"]
        }
        for config in filters:
            label = f"Filter {asset['dashboard_title']} / {config['name']}"
            if config["filterType"] != expected[config["id"]]:
                raise BootstrapError(f"{label}: unexpected filter type.")
            for target in config["targets"]:
                dataset_id = target["datasetId"]
                column = target["column"]["name"]
                result = run_query(
                    superset,
                    {
                        "datasource": {"id": dataset_id, "type": "table"},
                        # An empty metrics list groups options and resolves calculated columns.
                        "queries": [
                            {"columns": [column], "metrics": [], "row_limit": 1}
                        ],
                    },
                    label,
                )
                print(f"{label}: {column} query returned {result[0]['rowcount']} rows")


def check_dashboard_datasets(superset, assets, inventory):
    for uuid, asset in assets.items["dashboard"].items():
        dashboard_id = inventory["dashboard", uuid]["id"]
        label = f"Dashboard {asset['dashboard_title']} datasource metadata"
        # The dashboard UI needs this endpoint to configure its native filters.
        superset.api("GET", f"dashboard/{dashboard_id}/datasets", label)


def main():
    root = Path(__file__).resolve().parents[1] / "assets"
    assets = Assets(Path(os.getenv("SUPERSET_ASSETS_DIR", root)))
    expected = {(kind, uuid) for kind, items in assets.items.items() for uuid in items}
    with requests.Session() as session:
        session.trust_env = False
        superset = Superset(session)
        superset.login()
        superset.csrf()
        inventory = superset.inventory()
        missing = sorted(expected - inventory.keys())
        if missing:
            raise BootstrapError(f"Missing Superset asset UUIDs: {missing}")
        verify_import(superset, assets, inventory, expected)
        check_chart_queries(superset, assets, inventory)
        check_filter_queries(superset, assets, inventory)
        check_dashboard_datasets(superset, assets, inventory)
    print(
        "Superset smoke passed: 1 database, 3 datasets, 21 metrics, 18 charts, "
        "3 published dashboards and 7 native filters."
    )


if __name__ == "__main__":
    try:
        main()
    except BootstrapError as error:
        sys.exit(f"Superset smoke failed: {error}")
    except (KeyError, OSError, TypeError, ValueError) as error:
        sys.exit(
            f"Superset smoke failed: invalid asset or API data ({type(error).__name__})."
        )
