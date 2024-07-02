from flask import Flask
from healthcheck import HealthCheck
from prometheus_client import generate_latest
import io
import logging

from utils.logging import set_logging_configuration, APP_RUNNING
from utils.config import DEBUG, PORT, POD_NAME
from job_endpoints import job_api_bp
from jobs.frontdesk_borgerservice import connectToFrontdeskDB

import custom_data_connector as cdc

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
conn, df = connectToFrontdeskDB()

output = io.BytesIO()
df.to_csv(output, index=False)  # Write the DataFrame to a buffer
output.seek(0)  # Reset the buffer position to the start
# memory_view = memoryview(output.getvalue())  # Create a memory view of the buffer
# logger.info(df.head())
# logger.info(output)

cdc.post_data_to_custom_data_connector("Operations.csv", output)  # Post the buffer to the custom data connector


if __name__ == "__main__":  # pragma: no cover
    app.run(debug=DEBUG, host='0.0.0.0', port=PORT)
