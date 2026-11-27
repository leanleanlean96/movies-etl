import os
import subprocess

from superset.app import create_app
from superset.extensions import appbuilder


def main():
    password = os.environ["SUPERSET_ADMIN_PASSWORD"]
    if not password:
        raise RuntimeError("Set SUPERSET_ADMIN_PASSWORD before starting Superset")

    subprocess.run(["superset", "db", "upgrade"], check=True)
    subprocess.run(["superset", "init"], check=True)

    with create_app().app_context():
        security = appbuilder.sm
        if security.find_user(username="admin") is None:
            admin = security.add_user(
                username="admin",
                first_name="Superset",
                last_name="Admin",
                email="admin@example.com",
                role=security.find_role("Admin"),
                password=password,
            )
            if not admin:
                raise RuntimeError("Could not create Superset admin")


if __name__ == "__main__":
    main()
