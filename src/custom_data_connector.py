import io
import requests
import logging
import urllib.parse

from utils.config import CUSTOM_DATA_CONNECTOR_HOST


logger = logging.getLogger(__name__)


def post_data_to_custom_data_connector(filename, file):
    if file is None or filename is None:
        logger.error('No file or filename provided')
        return False

    if isinstance(file, io.BytesIO):
        file = file.getbuffer()
    if isinstance(file, io.BufferedReader):
        file = file.read()
    elif isinstance(file, memoryview):
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
        r = requests.post('http://' + CUSTOM_DATA_CONNECTOR_HOST + '/in', files=multipart_form_data, headers=headers)

        if r.ok:
            logger.info(filename + ' uploaded to custom-data-connector')
            return True
        else:
            raise Exception('Failed to upload ' + filename, r.text, r.status_code)

    except Exception as e:
        logger.error(e)
        return False
