from flask import Flask
from healthcheck import HealthCheck
from prometheus_client import generate_latest
import logging
from utils.database_connection import get_asset_db
from capa.capa_data import create_capa_table_if_not_exists

from utils.logging import set_logging_configuration, APP_RUNNING
from utils.config import DEBUG, PORT, POD_NAME
from job_endpoints import job_api_bp

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
initialize_db()
app = create_app()

if __name__ == "__main__":  # pragma: no cover
    app.run(debug=DEBUG, host='0.0.0.0', port=PORT)
