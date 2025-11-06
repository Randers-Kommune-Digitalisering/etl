from flask import Flask
from healthcheck import HealthCheck
from prometheus_client import generate_latest
import logging
from utils.database_connection import get_asset_db
from capa.capa_data import create_capa_table_if_not_exists

from utils.logging import set_logging_configuration, APP_RUNNING
from utils.config import DEBUG, PORT, POD_NAME
from job_endpoints import job_api_bp
import pandas as pd

logger = logging.getLogger(__name__)


def create_app():
    app = Flask('ETL')
    health = HealthCheck()
    app.add_url_rule("/healthz", "healthcheck", view_func=lambda: health.run())
    app.add_url_rule('/metrics', "metrics", view_func=generate_latest)
    app.register_blueprint(job_api_bp)
    APP_RUNNING.labels(POD_NAME).set(1)
    return app


def initialize_db():
    asset_db_client = get_asset_db()
    create_capa_table_if_not_exists(asset_db_client)
    logger.info("Initialization complete.")


set_logging_configuration()
app = create_app()

if __name__ == "__main__":  # pragma: no cover
    # initialize_db()
    # app.run(debug=DEBUG, host='0.0.0.0', port=PORT)
    from sd_delta import delta_client
    import json

    orgs_to_save = []
    orgs = delta_client.get_adm_orgs()
    for org in orgs[:2]:
        if org['name'].lower().strip() not in ['politikerne', 'borgmesterkontoret', 'handicaprådet', 'eksterne']:
            employees_leaders = delta_client.get_employees_and_leaders_with_user_by_adm_org(org['userkey'])
            if len(employees_leaders) > 1:
                orgs_to_save.append({"TeamName": org['name'], "Users": employees_leaders})
    with open("teams.json", "w", encoding="utf-8") as f:
        json.dump(orgs_to_save, f, indent=4, ensure_ascii=False)

    # with open("teams_new.json", "r", encoding="utf-8") as f:
    #     orgs = json.load(f)
    #     new_orgs = []
    #     for org in orgs:
    #         users = [user for user in org['Users'] if user.get('SamAccountName') is not None]

    #         if len(users) > 1:
    #             new_org = {"TeamName": org['TeamName'], "Users": users}
    #             new_orgs.append(new_org)

    #     with open("teams.json", "w", encoding="utf-8") as out_f:
    #         json.dump(new_orgs, out_f, indent=4, ensure_ascii=False)
