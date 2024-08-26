import pandas as pd
import requests
import json
import logging
from io import StringIO
from datetime import datetime


from utils.config import JOBINDSATS_API_KEY
from custom_data_connector import post_data_to_custom_data_connector


logger = logging.getLogger(__name__)


def job():
    logger.info('Starting jobindsats job')

    try:
        subjects = apiRequest('https://api.jobindsats.dk/v2/subjects/')
        tables = apiRequest('https://api.jobindsats.dk/v2/tables/')
        logger.info(tables.head())

        # Gem data lokalt - skal fjernes inden deployment
        df_to_csv(subjects, "subjects")
        df_to_csv(tables, "tables")

    except Exception as e:
        logger.error(f'Error: {e}')
        return False
    else:
        return True


def apiRequest(endpoint):
    response = requests.get(endpoint, headers={'Authorization': JOBINDSATS_API_KEY})
    json_data = response.json()
    df = pd.DataFrame(json_data)

    return df


def df_to_csv(df, filename):
    df.to_csv(f"{filename}.csv", index=False)
