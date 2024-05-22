from flask import Flask
from healthcheck import HealthCheck
from prometheus_client import generate_latest

from utils.logging import set_logging_configuration, APP_RUNNING
from utils.config import DEBUG, PORT, POD_NAME
from background_job import test_job
# from database import test_database


def create_app():
    app = Flask(__name__)
    health = HealthCheck()
    app.add_url_rule("/healthz", "healthcheck", view_func=lambda: health.run())
    app.add_url_rule('/metrics', "metrics", view_func=generate_latest)
    APP_RUNNING.labels(POD_NAME).set(1)

    @app.route('/test-job', methods=['GET'])
    def call_test_job():
        if test_job():
            return 'done', 200
        else:
            'failed', 500

    # @app.route('/test-database', methods=['GET'])
    # def test_database():
    #     ok = test_database()
    #     if ok:
    #         app.logger.info('Database ok')
    #         return ok
    #     return 'failed', 500
    return app


set_logging_configuration()
app = create_app()


if __name__ == "__main__":  # pragma: no cover
    # scheduler.start()
    app.run(debug=DEBUG, host='0.0.0.0', port=PORT)
