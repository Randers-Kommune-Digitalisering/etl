import io
import logging
from datetime import datetime

import pandas as pd
from utils.api_requests import APIClient
from utils.config import JOBINDSATS_API_KEY
from custom_data_connector import post_data_to_custom_data_connector

logger = logging.getLogger(__name__)

base_url = "https://api.jobindsats.dk"  # TODO: move to config
jobindsats_client = APIClient(base_url, JOBINDSATS_API_KEY)


def get_data(name, years_back, dataset, data_to_get):
    try:
        logger.info(f"Starting jobindsats: {name}")
        latest_period = period_request(dataset)
        period = dynamic_period(latest_period, years_back)
        payload = {"area": "*", "period": period} | data_to_get

        data = jobindsats_client.make_request(path=f'v2/data/{dataset}/json', json=payload)

        variables = data[0]['Variables']
        data = data[0]['Data']

        column_names = [var['Label'] for var in variables]

        df = pd.DataFrame(data, columns=column_names)

        df[f'Periode {name}'] = df['Periode'].apply(convert_to_datetime)  # TODO: Why can it not be called just "Periode"?

        df["Periode"] = df["Periode"]  # TODO: what is this for?

        df.rename(columns={"Area": "Område"}, inplace=True)

        file = io.BytesIO(df.to_csv(index=False, sep=';').encode('utf-8'))
        filename = f"SAJobindsats{dataset.replace('_', '').upper()}.csv"

        if post_data_to_custom_data_connector(filename, file):
            logger.info(f"Successfully updated {filename}")
            return True
        else:
            return False

    except Exception as e:
        logger.error(f'Error {e}')
        return False


# TODO: error handling?
def dynamic_period(latest_period, years_back):
    current_year = int(latest_period[:4])
    current_month = int(latest_period[5:])
    period = []

    for month in range(1, current_month + 1):
        period.append(f"{current_year}M{month:02d}")

    if years_back:
        for year in range(current_year - years_back, current_year):
            for month in range(1, 13):
                period.append(f"{year}M{month:02d}")

    return period


def convert_to_datetime(period_str):
    year = int(period_str[:4])
    month = int(period_str[5:])
    return datetime(year, month, 1)


# TODO: error handling?
def period_request(dataset):
    data = jobindsats_client.make_request(path=f'v2/tables/{dataset}/json/')
    periods = data[0]['Period']
    valid_periods = [p for p in periods if len(p) == 7 and p[4] == 'M' and p[5:].isdigit()]
    latest_period = max(valid_periods)
    return latest_period
