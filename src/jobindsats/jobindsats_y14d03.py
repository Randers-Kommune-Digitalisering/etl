import io
import logging
from datetime import datetime

import pandas as pd
from utils.api_requests import APIClient
from utils.config import JOBINDSATS_API_KEY
from custom_data_connector import post_data_to_custom_data_connector

logger = logging.getLogger(__name__)
base_url = "https://api.jobindsats.dk/v2/data/y14d03/json"
jobindsats_client = APIClient(base_url, JOBINDSATS_API_KEY)
period_url = "https://api.jobindsats.dk/v2/tables/y14d03/json/"
jobindsats_period_client = APIClient(period_url, JOBINDSATS_API_KEY)


# TODO: This function should also be made generic
def get_jobindsats_ydelse_til_job():
    try:
        logger.info("Starting jobindsats Fra ydelse til job")
        latest_period = period_request()
        period = dynamic_period(latest_period)
        payload = {
            "area": "*",
            "period": period,
            "_ygrpmm12": [
                "A-dagpenge",
                "Kontanthjælp",
                "Sygedagpenge"
            ],
            "_tilbud_d03": [
                "Privat virksomhedspraktik",
                "Offentlig virksomhedspraktik"
            ],
            "_maalgrp_d03": [
                "Jobparate mv.",
                "Aktivitetsparate mv."
            ],
        }
        data = jobindsats_client.make_request(json=payload)

        variables = data[0]['Variables']
        data = data[0]['Data']

        column_names = [var['Label'] for var in variables]

        df = pd.DataFrame(data, columns=column_names)
        df['Periode ydelse til job'] = df['Periode'].apply(convert_to_datetime)

        df["Periode"] = df["Periode"]

        df.rename(columns={"Area": "Område"}, inplace=True)

        file = io.BytesIO(df.to_csv(index=False, sep=';').encode('utf-8'))
        filename = "SA" + "JobindsatsY14D03" + ".csv"

        if post_data_to_custom_data_connector(filename, file):
            logger.info("Successfully updated JobindsatsY14D03")
            return True
        else:
            return False

    except Exception as e:
        logger.error(f'Error {e}')
        return False


def dynamic_period(latest_period):
    year_2024 = int(latest_period[:4])
    current_month = int(latest_period[8:])
    year_2023 = year_2024 - 1
    year_2022 = year_2023 - 1
    period = []

    for month in range(1, 13):
        period.append(f"{year_2023}QMAT{month:02d}")

    for month in range(1, current_month + 1):
        period.append(f"{year_2024}QMAT{month:02d}")

    for month in range(1, 13):
        period.append(f"{year_2022}QMAT{month:02d}")

    return period


def convert_to_datetime(period_str):
    year = int(period_str[:4])
    quarter = period_str[4:]

    quarter_to_month = {
        'QMAT01': 1,  # January Q1
        'QMAT02': 4,  # April Q2
        'QMAT03': 7,  # July Q3
        'QMAT04': 10  # October Q4
    }

    month = quarter_to_month.get(quarter)
    return datetime(year, month, 1)


def period_request():
    data = jobindsats_period_client.make_request()
    periods = data[0]['Period']
    valid_periods = [p for p in periods if len(p) == 10 and p[4:8] == 'QMAT' and p[8:].isdigit()]
    latest_period = max(valid_periods)
    return latest_period
