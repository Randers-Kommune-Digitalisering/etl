import os
import pathlib

from dotenv import load_dotenv


# loads .env file, will not overide already set enviroment variables (will do nothing when testing, building and deploying)
load_dotenv()


DEBUG = os.getenv('DEBUG', 'False') in ['True', 'true']
PORT = os.getenv('PORT', '8080')
POD_NAME = os.getenv('POD_NAME', 'Pod name not set')

SERVICEPLATFORM_SFTP_REMOTE_DIR = 'IN'
SERVICEPLATFORM_SFTP_HOST = os.environ['SERVICEPLATFORM_SFTP_HOST'].rstrip()
SERVICEPLATFORM_SFTP_USER = os.environ['SERVICEPLATFORM_SFTP_USER'].rstrip()
SERVICEPLATFORM_SSH_KEY_BASE64 = os.environ['SERVICEPLATFORM_SSH_KEY_BASE64'].rstrip()
SERVICEPLATFORM_SSH_KEY_PASS = os.environ['SERVICEPLATFORM_SSH_KEY_PASS'].rstrip()

TRUELINK_SFTP_REMOTE_DIR = '.'
TRUELINK_SFTP_HOST = os.environ['TRUELINK_SFTP_HOST'].rstrip()
TRUELINK_SFTP_USER = os.environ['TRUELINK_SFTP_USER'].rstrip()
TRUELINK_SSH_KEY_BASE64 = os.environ['TRUELINK_SSH_KEY_BASE64'].rstrip()
TRUELINK_SSH_KEY_PASS = None

CLIMATE_DB_USER = os.environ['CLIMATE_DB_USER'].rstrip()
CLIMATE_DB_PASS = os.environ['CLIMATE_DB_PASS'].rstrip()
CLIMATE_DB_HOST = os.environ['CLIMATE_DB_HOST'].rstrip()
CLIMATE_DB_PORT = os.environ['CLIMATE_DB_PORT'].rstrip()
CLIMATE_DB_DATABASE = os.environ['CLIMATE_DB_DATABASE'].rstrip()

FRONTDESK_DB_USER = os.environ['FRONTDESK_DB_USER'].rstrip()
FRONTDESK_DB_PASS = os.environ['FRONTDESK_DB_PASS'].rstrip()
FRONTDESK_DB_HOST = os.environ['FRONTDESK_DB_HOST'].rstrip()
FRONTDESK_DB_PORT = None
FRONTDESK_DB_DATABASE = os.environ['FRONTDESK_DB_DATABASE'].rstrip()

UDDANNELSESSTATISTIK_URL = os.environ['UDDANNELSESSTATISTIK_URL'].rstrip()
UDDANNELSESSTATISTIK_API_KEY = os.environ['UDDANNELSESSTATISTIK_API_KEY'].rstrip()

CUSTOM_DATA_CONNECTOR_HOST = os.environ['CUSTOM_DATA_CONNECTOR_HOST'].rstrip()

SD_URL = os.environ['SD_URL'].rstrip()
SD_USER = os.environ['SD_USER'].rstrip()
SD_PASS = os.environ['SD_PASS'].rstrip()

LOGIVA_URL = os.environ['LOGIVA_URL'].rstrip()
LOGIVA_USER = os.environ['LOGIVA_USER'].rstrip()
LOGIVA_PASS = os.environ['LOGIVA_PASS'].rstrip()

SD_DELTA_EXCLUDED_DEPARTMENTS_FILE_PATH = os.path.join(pathlib.Path(__file__).parent.resolve(), 'sd_delta_excluded_units.csv')
