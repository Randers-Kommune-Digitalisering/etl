import os
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

JOBINDSATS_API_KEY = os.environ['JOBINDSATS_API_KEY'].rstrip()
SENSUM_IT_SFTP_REMOTE_DIR = os.environ['SENSUM_IT_SFTP_REMOTE_DIR'].rstrip() 
SENSUM_IT_SFTP_HOST = os.environ['SENSUM_IT_SFTP_HOST'].rstrip()
SENSUM_IT_SFTP_USER = os.environ['SENSUM_IT_SFTP_USER'].rstrip()
SENSUM_IT_SFTP_PASS = os.environ['SENSUM_IT_SFTP_PASS'].rstrip()