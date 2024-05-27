import os
from dotenv import load_dotenv


# loads .env file, will not overide already set enviroment variables (will do nothing when testing, building and deploying)
load_dotenv()


DEBUG = os.getenv('DEBUG', 'False') in ['True', 'true']
PORT = os.getenv('PORT', '8080')
POD_NAME = os.getenv('POD_NAME', 'Pod name not set')

SERVICEPLATFORM_SFTP_REMOTE_DIR = 'IN'
SERVICEPLATFORM_SFTP_HOST = os.environ['SERVICEPLATFORM_SFTP_HOST']
SERVICEPLATFORM_SFTP_USER = os.environ['SERVICEPLATFORM_SFTP_USER']
SERVICEPLATFORM_SSH_KEY_BASE64 = os.environ['SERVICEPLATFORM_SSH_KEY_BASE64']
SERVICEPLATFORM_SSH_KEY_PASS = os.environ['SERVICEPLATFORM_SSH_KEY_PASS']

TRUELINK_SFTP_REMOTE_DIR = '.'
TRUELINK_SFTP_HOST = os.environ['TRUELINK_SFTP_HOST']
TRUELINK_SFTP_USER = os.environ['TRUELINK_SFTP_USER']
TRUELINK_SSH_KEY_BASE64 = os.environ['TRUELINK_SSH_KEY_BASE64']
TRUELINK_SSH_KEY_PASS = None

CLIMATE_DB_USER = os.environ['CLIMATE_DB_USER']
CLIMATE_DB_PASS = os.environ['CLIMATE_DB_PASS']
CLIMATE_DB_HOST = os.environ['CLIMATE_DB_HOST']
CLIMATE_DB_PORT = os.environ['CLIMATE_DB_PORT']
CLIMATE_DB_DATABASE = os.environ['CLIMATE_DB_DATABASE']

FRONTDESK_DB_USER = os.environ['FRONTDESK_DB_USER']
FRONTDESK_DB_PASS = os.environ['FRONTDESK_DB_PASS']
FRONTDESK_DB_HOST = os.environ['FRONTDESK_DB_HOST']
FRONTDESK_DB_PORT = None
FRONTDESK_DB_DATABASE = os.environ['FRONTDESK_DB_DATABASE']

CUSTOM_DATA_CONNECTOR_HOST = os.environ['CUSTOM_DATA_CONNECTOR_HOST']
