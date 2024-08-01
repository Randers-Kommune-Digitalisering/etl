from flask import Flask
from healthcheck import HealthCheck
from prometheus_client import generate_latest
import logging

from utils.logging import set_logging_configuration, APP_RUNNING
from config.config import DEBUG, PORT, POD_NAME
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


set_logging_configuration()
app = create_app()

if __name__ == "__main__":  # pragma: no cover
    app.run(debug=DEBUG, host='0.0.0.0', port=PORT)
