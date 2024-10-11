import io
import logging
from datetime import datetime

import pandas as pd
from utils.api_requests import APIClient
from utils.config import JOBINDSATS_API_KEY
from custom_data_connector import post_data_to_custom_data_connector

logger = logging.getLogger(__name__)
base_url = "https://api.jobindsats.dk/v2/data/ptvc01/json"
jobindsats_client = APIClient(base_url, JOBINDSATS_API_KEY)


def get_jobindsats_tilbud_samtaler():
    try:
        logger.info("Starting jobindsats Tilbud og samtaler")
        period = dynamic_period()
        payload = {
            "area": "*",
            "period": period,
            "_ygrpc02": [
                "A-dagpenge",
                "Kontanthjælp",
                "Uddannelseshjælp",
                "Selvforsørgelses- og hjemrejseydelse mv.",
                "Sygedagpenge"
            ],
        }
        data = jobindsats_client.make_request(json=payload)

        variables = data[0]['Variables']
        data = data[0]['Data']

        column_names = [var['Label'] for var in variables]

        df = pd.DataFrame(data, columns=column_names)
        df['Periode tilbud og samtaler'] = df['Periode'].apply(convert_to_datetime)

        df["Periode"] = df["Periode"]

        # Rename column name
        df.rename(columns={"Area": "Område"}, inplace=True)

        file = io.BytesIO(df.to_csv(index=False, sep=';').encode('utf-8'))
        filename = "SA" + "JobindsatsPTVC01" + ".csv"

        if post_data_to_custom_data_connector(filename, file):
            logger.info("Successfully updated JobindsatsPTVC01")
            return True
        else:
            return False

    except Exception as e:
        logger.error(f'Error {e}')
        return False


def dynamic_period():
    current_year = datetime.now().year - 1
    previous_year = current_year - 1
    period = [
        f"{previous_year}Y01", f"{current_year}Y01"
    ]
    return period


def convert_to_datetime(period_str):  # Convert period string to datetime
    year = int(period_str[:4])
    month = int(period_str[5:])
    return datetime(year, month, 1)