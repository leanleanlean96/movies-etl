"""Import the versioned Superset dashboards after the daily ClickHouse publication."""

import argparse
import io
import json
import os
import sys
import time
from pathlib import Path
from urllib.parse import quote, urlsplit
from uuid import UUID
from zipfile import ZIP_DEFLATED, ZipFile

import requests
import yaml

CONTRACT = (1, 3, 21, 18, 3, 7)
KINDS = ("database", "dataset", "chart", "dashboard")
# The narrower imports use the Superset model class name in metadata.yaml.
IMPORT_TYPES = {
    "database": "Database",
    "dataset": "SqlaTable",
    "chart": "Slice",
    "assets": "assets",
}
PAGE_SIZE = 100
POLL_SECONDS = 10
READ_TIMEOUT = 8
IMPORT_TIMEOUT = 60


class BootstrapError(Exception):
    """An error whose message is safe to include in the bootstrap log."""


class ApiError(BootstrapError):
    def __init__(self, stage, status=None):
        self.stage = stage
        self.status = status
        reason = f"HTTP {status}" if status is not None else "transport error"
        super().__init__(f"{stage}: {reason}")

    @property
    def transient(self):
        return (
            self.status is None or self.status in (408, 425, 429) or self.status >= 500
        )


def request(session, method, url, stage, *, timeout=READ_TIMEOUT, **kwargs):
    try:
        response = session.request(
            method, url, timeout=timeout, allow_redirects=False, **kwargs
        )
    except requests.RequestException:
        raise ApiError(stage) from None
    if not 200 <= response.status_code < 300:
        raise ApiError(stage, response.status_code)
    return response


def credential(name):
    value = os.getenv(name)
    if not value:
        raise BootstrapError(f"Set {name} before starting the BI bootstrap.")
    return value


def log(message):
    print(f"BI bootstrap: {message}", flush=True)


def has_successful_publication(get, runs_url, task):
    offset = 0
    while True:
        # Publication is the final task. A running DAG can already have
        # published before the scheduler marks the run successful.
        page = get(
            runs_url,
            "Airflow daily runs",
            params={
                "state": ["success", "running"],
                "order_by": "-id",
                "limit": PAGE_SIZE,
                "offset": offset,
            },
        ).json()
        runs = page["dag_runs"]
        for run in runs:
            run_id = quote(run["dag_run_id"], safe="")
            try:
                instance = get(
                    f"{runs_url}/{run_id}/taskInstances/{task}",
                    "Airflow publication task",
                ).json()
            except ApiError as error:
                if error.status == 404:
                    continue
                raise
            if instance["state"] == "success":
                return True
        offset += len(runs)
        if not runs or offset >= page["total_entries"]:
            return False


def wait_for_publication():
    try:
        seconds = int(os.getenv("BI_BOOTSTRAP_TIMEOUT_SECONDS", "7200"))
    except ValueError:
        raise BootstrapError(
            "BI_BOOTSTRAP_TIMEOUT_SECONDS must be a positive integer."
        ) from None
    if seconds <= 0:
        raise BootstrapError("BI_BOOTSTRAP_TIMEOUT_SECONDS must be a positive integer.")

    deadline = time.monotonic() + seconds
    airflow_url = os.getenv("AIRFLOW_URL", "http://airflow-api-server:8080").rstrip("/")
    airflow_login = {
        "username": os.getenv("AIRFLOW_ADMIN_USERNAME", "admin"),
        "password": credential("AIRFLOW_ADMIN_PASSWORD"),
    }
    dag = quote(os.getenv("AIRFLOW_DAG_ID", "kinopoisk_movies_daily"), safe="")
    task = quote(os.getenv("AIRFLOW_PUBLISH_TASK_ID", "publish_clickhouse"), safe="")
    runs_url = f"{airflow_url}/api/v2/dags/{dag}/dagRuns"
    clickhouse_url = os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123").rstrip("/")
    reader = (
        os.getenv("CLICKHOUSE_READER_USER", "superset_reader"),
        credential("CLICKHOUSE_READER_PASSWORD"),
    )
    database = os.getenv("CLICKHOUSE_DATABASE", "mart")
    published = False
    previous = ""
    status = "waiting for the Airflow API"

    def poll(session, method, url, stage, **kwargs):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise BootstrapError(f"Readiness timed out after {seconds}s ({status}).")
        return request(
            session, method, url, stage, timeout=min(READ_TIMEOUT, remaining), **kwargs
        )

    log(f"waiting up to {seconds}s for the daily publication and catalog rows.")
    with requests.Session() as airflow, requests.Session() as clickhouse:
        airflow.trust_env = clickhouse.trust_env = False

        def airflow_get(url, stage, **kwargs):
            return poll(airflow, "GET", url, stage, **kwargs)

        while time.monotonic() < deadline:
            try:
                if not published:
                    if "Authorization" not in airflow.headers:
                        token = poll(
                            airflow,
                            "POST",
                            f"{airflow_url}/auth/token",
                            "Airflow authentication",
                            json=airflow_login,
                        ).json()["access_token"]
                        airflow.headers["Authorization"] = f"Bearer {token}"

                    status = "the daily DAG has no successful publication task yet"
                    published = has_successful_publication(airflow_get, runs_url, task)
                    if published:
                        log("Airflow reports a successful daily publication.")

                if published:
                    status = "ClickHouse catalog has no rows after publication"
                    probe = poll(
                        clickhouse,
                        "POST",
                        f"{clickhouse_url}/",
                        "ClickHouse catalog probe",
                        auth=reader,
                        params={"database": database, "wait_end_of_query": "1"},
                        data="SELECT 1 FROM mart_movie_scores_current LIMIT 1",
                    ).text.strip()
                    if probe == "1":
                        log(
                            "ClickHouse catalog contains data under the Superset reader."
                        )
                        return
            except ApiError as error:
                status = str(error)
                if error.stage.startswith("Airflow") and error.status == 401:
                    if airflow.headers.pop("Authorization", None) is None:
                        raise
                elif not (error.transient or error.status == 404):
                    raise

            if status != previous:
                log(f"waiting ({status}).")
                previous = status
            time.sleep(max(0, min(POLL_SECONDS, deadline - time.monotonic())))

    raise BootstrapError(f"Readiness timed out after {seconds}s ({status}).")


def chart_positions(position):
    return {
        node["meta"]["uuid"]: node["meta"]["chartId"]
        for node in position.values()
        if isinstance(node, dict)
        and node.get("type") == "CHART"
        and node["meta"].get("uuid")
    }


def native_filters(metadata):
    return metadata.get("native_filter_configuration") or []


class Assets:
    def __init__(self, root):
        self.files = {}
        self.items = {kind: {} for kind in KINDS}
        for kind in KINDS:
            for path in sorted((root / f"{kind}s").rglob("*.yaml")):
                source = path.read_bytes()
                obj = yaml.safe_load(source)
                uuid = str(UUID(obj["uuid"]))
                if uuid in self.items[kind]:
                    raise BootstrapError(f"Duplicate {kind} UUID in the asset bundle.")
                self.items[kind][uuid] = obj
                self.files[kind, uuid] = (path.relative_to(root).as_posix(), source)
        self.metadata = (root / "metadata.yaml").read_bytes()
        if yaml.safe_load(self.metadata).get("type") != "Dashboard":
            raise BootstrapError("Expected a Dashboard bundle in metadata.yaml.")
        self.validate()

    def validate(self):
        database, datasets, charts, dashboards = (self.items[kind] for kind in KINDS)
        counts = (
            len(database),
            len(datasets),
            sum(len(obj["metrics"]) for obj in datasets.values()),
            len(charts),
            len(dashboards),
            sum(len(native_filters(obj["metadata"])) for obj in dashboards.values()),
        )
        if counts != CONTRACT:
            raise BootstrapError(
                f"Unexpected asset bundle counts {counts}; expected {CONTRACT}."
            )
        if any(obj["published"] is not True for obj in dashboards.values()):
            raise BootstrapError("The asset bundle contains an unpublished dashboard.")

        for obj in database.values():
            password = urlsplit(obj["sqlalchemy_uri"]).password
            if password not in (None, "XXXXXXXXXX") or obj.get("password"):
                raise BootstrapError("Use a masked database URI in the asset bundle.")

        try:
            reachable = self.dependencies("dashboard", dashboards)
        except KeyError:
            raise BootstrapError(
                "The asset bundle refers to a missing dependency."
            ) from None
        expected = {(kind, uuid) for kind in KINDS for uuid in self.items[kind]}
        if reachable != expected:
            raise BootstrapError(
                "The asset bundle has missing or unreachable dependencies."
            )

    def dependencies(self, kind, uuids):
        """Follow the dependencies Superset's importers discover from the YAML."""
        selected = {kind: set(uuids)}
        if kind == "dashboard":
            dashboards = [self.items[kind][uuid] for uuid in uuids]
            selected["chart"] = {
                uuid for obj in dashboards for uuid in chart_positions(obj["position"])
            }
            selected["dataset"] = {
                target["datasetUuid"]
                for obj in dashboards
                for native_filter in native_filters(obj["metadata"])
                for target in native_filter["targets"]
            }
        if "chart" in selected:
            selected.setdefault("dataset", set()).update(
                self.items["chart"][uuid]["dataset_uuid"] for uuid in selected["chart"]
            )
        if "dataset" in selected:
            selected["database"] = {
                self.items["dataset"][uuid]["database_uuid"]
                for uuid in selected["dataset"]
            }
        return {(name, uuid) for name, ids in selected.items() for uuid in ids}

    def archive(self, kind, selected):
        metadata = self.metadata
        if kind != "dashboard":
            config = yaml.safe_load(metadata)
            config["type"] = IMPORT_TYPES[kind]
            metadata = yaml.safe_dump(config)
        output = io.BytesIO()
        passwords = {}
        with ZipFile(output, "w", ZIP_DEFLATED) as archive:
            archive.writestr("bundle/metadata.yaml", metadata)
            for key in sorted(selected):
                path, source = self.files[key]
                archive.writestr(f"bundle/{path}", source)
                if key[0] == "database":
                    # Superset strips the leading bundle directory before matching paths.
                    passwords[path] = credential("CLICKHOUSE_READER_PASSWORD")
        output.seek(0)
        return output, passwords


class Superset:
    def __init__(self, session):
        self.session = session
        self.url = os.getenv("SUPERSET_URL", "http://superset:8088").rstrip("/")

    def api(self, method, path, stage, **kwargs):
        return request(
            self.session, method, f"{self.url}/api/v1/{path}", stage, **kwargs
        )

    def login(self):
        result = self.api(
            "POST",
            "security/login",
            "Superset login",
            json={
                "username": os.getenv("SUPERSET_ADMIN_USERNAME", "admin"),
                "password": credential("SUPERSET_ADMIN_PASSWORD"),
                "provider": "db",
            },
        ).json()
        self.session.headers["Authorization"] = f"Bearer {result['access_token']}"

    def csrf(self):
        result = self.api("GET", "security/csrf_token/", "Superset CSRF").json()
        self.session.headers.update(
            {"X-CSRFToken": result["result"], "Referer": f"{self.url}/"}
        )

    def inventory(self):
        inventory = {}
        fields_by_kind = {
            "database": ["id", "uuid", "database_name"],
            "dataset": [
                "id",
                "uuid",
                "database.id",
                "database.uuid",
                "table_name",
                "schema",
                "catalog",
            ],
            "chart": ["id", "uuid", "datasource_id", "datasource_type"],
            "dashboard": ["id", "uuid", "published", "slug"],
        }
        for kind, fields in fields_by_kind.items():
            page = 0
            while True:
                result = self.api(
                    "GET",
                    f"{kind}/",
                    f"Superset {kind} inventory",
                    params={
                        "q": json.dumps(
                            {"columns": fields, "page": page, "page_size": PAGE_SIZE}
                        )
                    },
                ).json()
                for item in result["result"]:
                    inventory[kind, item["uuid"]] = item
                page += 1
                if not result["result"] or page * PAGE_SIZE >= result["count"]:
                    break
        return inventory

    def import_assets(self, assets, kind, selected):
        bundle, passwords = assets.archive(kind, selected)
        field = "bundle" if kind == "assets" else "formData"
        form = {"passwords": json.dumps(passwords)}
        if kind != "assets":
            form["overwrite"] = "false"
        self.api(
            "POST",
            f"{kind}/import/",
            f"Superset {kind} import",
            timeout=IMPORT_TIMEOUT,
            data=form,
            files={field: ("etl-pet-bi.zip", bundle, "application/zip")},
        )

    def query_dataset(self, dataset_id):
        result = self.api(
            "POST",
            "chart/data",
            "Superset dataset query",
            json={
                "datasource": {"id": dataset_id, "type": "table"},
                "force": True,
                "queries": [
                    {
                        "metrics": [
                            {
                                "expressionType": "SQL",
                                "sqlExpression": "COUNT(*)",
                                "label": "bootstrap_row_count",
                            }
                        ],
                        "row_limit": 1,
                    }
                ],
                "result_format": "json",
                "result_type": "full",
            },
        ).json()["result"]
        if len(result) != 1 or result[0]["status"] != "success":
            raise BootstrapError(
                f"Superset dataset query failed for dataset ID {dataset_id}."
            )

    def dashboard_graph(self, dashboard_id):
        detail = self.api(
            "GET", f"dashboard/{dashboard_id}", "Superset dashboard details"
        ).json()["result"]
        related = self.api(
            "GET", f"dashboard/{dashboard_id}/charts", "Superset dashboard charts"
        ).json()["result"]
        return (
            chart_positions(json.loads(detail["position_json"] or "{}")),
            {chart["id"] for chart in related},
            native_filters(json.loads(detail["json_metadata"] or "{}")),
        )


def verify_import(superset, assets, inventory, verify):
    def object_id(kind, uuid):
        return inventory[kind, uuid]["id"]

    for kind, uuid in sorted(verify):
        expected = assets.items[kind][uuid]
        item = inventory[kind, uuid]
        item_id = item["id"]
        if kind == "dataset":
            detail = superset.api(
                "GET", f"dataset/{item_id}", "Superset dataset details"
            ).json()["result"]
            if item["database"]["id"] != object_id(
                "database", expected["database_uuid"]
            ):
                raise BootstrapError(f"Dataset {uuid} has an unexpected database.")
            if sorted(metric["metric_name"] for metric in detail["metrics"]) != sorted(
                metric["metric_name"] for metric in expected["metrics"]
            ):
                raise BootstrapError(f"Dataset {uuid} has unexpected saved metrics.")
            superset.query_dataset(item_id)
        elif kind == "chart":
            if (item["datasource_type"], item["datasource_id"]) != (
                "table",
                object_id("dataset", expected["dataset_uuid"]),
            ):
                raise BootstrapError(f"Chart {uuid} has an unexpected dataset.")
        elif kind == "dashboard":
            if item["published"] is not True:
                raise BootstrapError(f"Dashboard {uuid} is unpublished.")
            positions, related, filters = superset.dashboard_graph(item_id)
            chart_ids = {
                chart: object_id("chart", chart)
                for chart in chart_positions(expected["position"])
            }
            if positions != chart_ids or related != set(chart_ids.values()):
                raise BootstrapError(
                    f"Dashboard {uuid} has unexpected chart relationships."
                )

            targets = {
                config["id"]: [
                    (
                        object_id("dataset", target["datasetUuid"]),
                        target["column"]["name"],
                    )
                    for target in config["targets"]
                ]
                for config in native_filters(expected["metadata"])
            }
            actual = {
                config["id"]: [
                    (target["datasetId"], target["column"]["name"])
                    for target in config["targets"]
                ]
                for config in filters
            }
            if actual != targets or len(filters) != len(targets):
                raise BootstrapError(f"Dashboard {uuid} has unexpected native filters.")


def verify_existing_dashboards(superset, inventory, existing):
    # A recreated chart gets a new numeric ID. Check the live layout while
    # preserving the user's choice of which charts appear on the dashboard.
    chart_ids = {
        uuid: item["id"] for (kind, uuid), item in inventory.items() if kind == "chart"
    }
    dataset_ids = {
        item["id"] for (kind, _), item in inventory.items() if kind == "dataset"
    }
    missing_datasets = set()
    broken = False
    for (kind, uuid), item in existing.items():
        if kind != "dashboard":
            continue
        positions, related, filters = superset.dashboard_graph(item["id"])
        for chart, chart_id in positions.items():
            if chart in chart_ids and (
                chart_ids[chart] != chart_id or chart_id not in related
            ):
                log(
                    f"dashboard {uuid} needs its existing layout relinked to chart {chart}."
                )
                broken = True
        targets = {
            target["datasetId"]
            for config in filters
            for target in config.get("targets", [])
            if isinstance(target.get("datasetId"), int)
        }
        for dataset_id in targets - dataset_ids - missing_datasets:
            # The UI may have changed a filter to use an unmanaged dataset.
            try:
                superset.api("GET", f"dataset/{dataset_id}", "Superset filter dataset")
            except ApiError as error:
                if error.status != 404:
                    raise
                missing_datasets.add(dataset_id)
            else:
                dataset_ids.add(dataset_id)
        for dataset_id in sorted(targets & missing_datasets):
            log(
                f"dashboard {uuid} has a native filter pointing to missing dataset ID {dataset_id}."
            )
            broken = True
    if broken:
        raise BootstrapError(
            "Managed UUIDs are present, but existing dashboard links need repair."
        )


def check_import_conflicts(assets, inventory, selected, overwrite):
    # Superset can also match an object by its unique metadata fields.
    # Guard those matches so a manually created object keeps its UUID.
    for key in sorted(selected):
        kind, uuid = key
        if not overwrite and key in inventory:
            continue
        obj = assets.items[kind][uuid]
        if kind == "database":
            fields = {"database_name": obj["database_name"]}
        elif kind == "dataset":
            fields = {
                field: obj[field]
                for field in ("table_name", "schema", "catalog")
                if obj.get(field) is not None
            }
            fields["database"] = obj["database_uuid"]
        elif kind == "dashboard" and obj.get("slug"):
            fields = {"slug": obj["slug"]}
        else:
            continue
        for (other_kind, other_uuid), current in inventory.items():
            if other_kind != kind or other_uuid == uuid:
                continue
            if kind == "dataset":
                current = {**current, "database": current["database"]["uuid"]}
            if all(current[field] == value for field, value in fields.items()):
                raise BootstrapError(
                    f"Superset already has a {kind} with the same unique fields as {uuid} and a different UUID."
                )


def reconcile_import(superset, assets, inventory, kind, selected, required):
    check_import_conflicts(assets, inventory, selected, overwrite=kind == "assets")
    try:
        superset.import_assets(assets, kind, selected)
    except ApiError as error:
        if not (error.transient or error.status in (409, 422)):
            raise
        if kind == "assets":
            if not error.transient:
                raise
            raise BootstrapError(
                f"{error}. Overwrite outcome is unknown; inspect Superset before rerunning."
            ) from None

        # A timed-out request may still be committing. Read the expected UUIDs
        # again before deciding the outcome. No import POST is replayed here.
        log(f"{error}; checking UUIDs for a possible committed import.")
        for attempt in range(3 if error.transient else 1):
            if attempt:
                time.sleep(2)
            try:
                inventory = superset.inventory()
            except ApiError as read_error:
                if not read_error.transient:
                    raise
                continue
            if required <= inventory.keys():
                log(
                    "Superset contains the requested UUIDs after the uncertain response."
                )
                return inventory
        if not error.transient:
            raise
        raise BootstrapError(
            f"{error}. Import outcome remains uncertain; rerun the normal bootstrap to reconcile."
        ) from None

    inventory = superset.inventory()
    if not required <= inventory.keys():
        raise BootstrapError(
            f"Superset {kind} import returned success with UUIDs still missing."
        )
    return inventory


def bootstrap(overwrite):
    wait_for_publication()
    with requests.Session() as session:
        session.trust_env = False
        superset = Superset(session)
        superset.login()
        assets_dir = os.getenv(
            "SUPERSET_ASSETS_DIR", Path(__file__).resolve().parents[1] / "assets"
        )
        assets = Assets(Path(assets_dir))
        expected = set(assets.files)
        inventory = superset.inventory()
        before = {key: inventory[key] for key in expected & inventory.keys()}
        missing = expected - before.keys()
        if not overwrite and not missing:
            verify_existing_dashboards(superset, inventory, before)
            for uuid in sorted(assets.items["dataset"]):
                superset.query_dataset(before["dataset", uuid]["id"])
            log("all managed UUIDs are present. Existing Superset edits were retained.")
            return

        superset.csrf()
        if overwrite:
            log("importing the complete assets bundle with overwrite.")
            after = reconcile_import(
                superset, assets, inventory, "assets", expected, expected
            )
        elif not before:
            log("importing the Dashboard bundle into a clean BI deployment.")
            after = reconcile_import(
                superset, assets, inventory, "dashboard", expected, expected
            )
        else:
            log("recovering missing UUIDs from a partial BI deployment.")
            after = inventory
            for kind in reversed(KINDS):
                uuids = {uuid for name, uuid in expected - after.keys() if name == kind}
                if uuids:
                    log(f"recovering {len(uuids)} missing {kind} object(s).")
                    after = reconcile_import(
                        superset,
                        assets,
                        after,
                        kind,
                        assets.dependencies(kind, uuids),
                        {(kind, uuid) for uuid in uuids},
                    )

        if not expected <= after.keys() or any(
            after[key]["id"] != entry["id"] for key, entry in before.items()
        ):
            raise BootstrapError(
                "Superset inventory changed during verification; rerun bootstrap."
            )
        verify_import(superset, assets, after, expected if overwrite else missing)
        if before and not overwrite:
            verify_existing_dashboards(superset, after, before)
            log(
                "missing objects recovered. Existing dashboard layouts and filters were retained."
            )
        else:
            log(
                "verified 1 database, 3 datasets, 21 metrics, 18 charts, "
                "3 published dashboards and 7 native filters; all dataset queries succeeded."
            )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--overwrite", action="store_true", help="Update the full BI asset bundle."
    )
    args = parser.parse_args()
    try:
        bootstrap(args.overwrite)
    except BootstrapError as error:
        log(f"ERROR: {error}")
        return 1
    except Exception as error:  # noqa: BLE001
        # API and YAML exceptions may contain response bodies, credentials or SQL.
        log(f"ERROR: invalid configuration or API response ({type(error).__name__}).")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
