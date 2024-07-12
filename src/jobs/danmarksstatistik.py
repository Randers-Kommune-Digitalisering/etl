import io
import logging
import datetime
import pandas as pd

from utils.config import DST_HOST_DATA
from utils.api_requests import APIClient
from custom_data_connector import post_data_to_custom_data_connector

logger = logging.getLogger(__name__)

dst_client = APIClient(DST_HOST_DATA)   

def job():
    logger.info('Handling Danmarks Statistik files')
    return handle_data('BIL54')

def handle_data(data):
    payload = {
        "table": "BIL54",
        "format": "CSV",
        "variables": [
            {
                "code": "TID",
                "values": [
                    "2024M06"
                ]
            },
            {
                "code": "BILTYPE",
                "values": [
                    "4000100001",
                    "*"
                ]
            }
        ]
    }
    
    try:
        data = dst_client.make_request(json=payload)
        df = pd.DataFrame(data)
        if data:
            file = io.BytesIO(df.to_csv(index=False, sep=';').encode('utf-8'))
            filename='BIL54.csv'

    except Exception as e:
        logger.error(e)
        return False

    if post_data_to_custom_data_connector(filename, file):
        logger.info('Updated "BIL54"')
        return True
    else:
        logger.error('Failed to update "BIL54"')
        return False
    