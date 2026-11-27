import os
from urllib.parse import quote

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
if not SECRET_KEY:
    raise RuntimeError("Set SUPERSET_SECRET_KEY before starting Superset")

SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://superset:{quote(os.environ['SUPERSET_DB_PASSWORD'], safe='')}"
    "@superset-db/superset"
)
