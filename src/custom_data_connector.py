import io
import logging
import urllib.parse

from utils.config import CUSTOM_DATA_CONNECTOR_HOST
from utils.api_requests import APIClient


logger = logging.getLogger(__name__)
api_client = APIClient(CUSTOM_DATA_CONNECTOR_HOST)


def post_data_to_custom_data_connector(filename, file):
    if file is None or filename is None:
        logger.error('No file or filename provided')
        return False

    if isinstance(file, io.BytesIO):
        file.seek(0)
        file = file.getbuffer()
    if isinstance(file, io.BufferedReader):
        file = file.read()
    elif isinstance(file, memoryview):
        pass
    elif isinstance(file, bytes):
        pass
    else:
        logger.error(f"File should be either io.BytesIO, io.BufferedReader, or memeoryview, but was {type(file)}")
        return False

    if not filename.endswith('.csv'):
        filename += '.csv'

    encoded_filename = urllib.parse.quote(filename)
    headers = {'overwrite': 'true'}

    multipart_form_data = {'file': (encoded_filename, file, 'text/csv')}

    try:
        api_client.make_request(path='in', files=multipart_form_data, headers=headers)
        logger.info(filename + ' uploaded to custom-data-connector')
        return True
    except Exception as e:
        logger.error(e)
        return False


def read_data_from_custom_data_connector(filename):
    if filename is None:
        logger.error('No file or filename provided')
        return False

    try:
        file = api_client.make_request(path=f'out/{filename}')
        logger.info(filename + ' downloaded from custom-data-connector')
        return file
    except Exception as e:
        logger.error(e)
        return False
