import io
import logging
from datetime import datetime
import requests
import pandas as pd
from utils.api_requests import APIClient
from utils.config import JOBINDSATS_API_KEY
from custom_data_connector import post_data_to_custom_data_connector

logger = logging.getLogger(__name__)
base_url = "https://api.jobindsats.dk/v2/data/y36a02/json"
jobindsats_client = APIClient(base_url, JOBINDSATS_API_KEY)
period_url = "https://api.jobindsats.dk/v2/tables/Y36A02/json/"
jobindsats_period_client = APIClient(period_url, JOBINDSATS_API_KEY)


def get_jobindsats_kontanthjælp():
    try:
        logger.info("Starting jobindsats Kontanthjælp")
        latest_period = period_request()
        period = dynamic_period(latest_period)
        payload = {
            "area": "*",
            "period": period,
            "_kon": [
                "Kvinder",
                "Mænd"
            ],
            "_oprinda": [
                "Personer med dansk oprindelse",
                "Indvandrere fra vestlige lande",
                "Efterkommere fra vestlige lande",
                "Indvandrere fra ikke-vestlige lande",
                "Efterkommere fra ikke-vestlige lande"
            ],
            "_viskat_1kth": [
                "Jobparat",
                "Aktivitetsparat",
                "Uoplyst visitationskategori"
            ]
        }
        data = jobindsats_client.make_request(json=payload)

        variables = data[0]['Variables']
        data = data[0]['Data']

        column_names = [var['Label'] for var in variables]

        df = pd.DataFrame(data, columns=column_names)
        df['Periode kontanthjælp'] = df['Periode'].apply(convert_to_datetime)

        df["Periode"] = df["Periode"]

        df.rename(columns={"Area": "Område"}, inplace=True)

        file = io.BytesIO(df.to_csv(index=False, sep=';').encode('utf-8'))
        filename = "SA" + "JobindsatsY36A02" + ".csv"

        if post_data_to_custom_data_connector(filename, file):
            logger.info("Successfully updated JobindsatsY36A02 ")
            return True
        else:
            return False

    except Exception as e:
        logger.error(f'Error {e}')
        return False


def dynamic_period(latest_period):
    current_year = int(latest_period[:4])
    current_month = int(latest_period[5:])
    previous_year = current_year - 1
    period = []

    for month in range(1, 13):
        period.append(f"{previous_year}M{month:02d}")

    for month in range(1, current_month + 1):
        period.append(f"{current_year}M{month:02d}")

    return period


def convert_to_datetime(period_str):
    year = int(period_str[:4])
    month = int(period_str[5:])
    return datetime(year, month, 1)


def period_request():
    data = jobindsats_period_client.make_request()
    periods = data[0]['Period']
    valid_periods = [p for p in periods if len(p) == 7 and p[4] == 'M' and p[5:].isdigit()]
    latest_period = max(valid_periods)
    return latest_period
