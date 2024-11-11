import io
import logging
from datetime import datetime
import pandas as pd
from utils.config import JOBINDSATS_API_KEY
from utils.api_requests import APIClient
from custom_data_connector import post_data_to_custom_data_connector

logger = logging.getLogger(__name__)
base_url = "https://api.jobindsats.dk/v2/data/y30r21/json"
jobindsats_client = APIClient(base_url, JOBINDSATS_API_KEY)
period_url = "https://api.jobindsats.dk/v2/tables/y30r21/json/"
jobindsats_period_client = APIClient(period_url, JOBINDSATS_API_KEY)


# TODO: This function should also be made generic
def get_jobindats_ydelsesgrupper():
    try:
        logger.info("Starting jobindsats Ydelsesgrupper")
        latest_period = period_request()
        period = dynamic_period(latest_period)
        payload = {
            "area": "*",
            "period": period,
            "_ygrp_y30r21": [
                "Ydelsesgrupper i alt",
                "A-dagpenge mv.",
                "Sygedagpenge mv.",
                "Kontanthjælp mv."
            ]
        }
        data = jobindsats_client.make_request(json=payload)

        variables = data[0]['Variables']
        data = data[0]['Data']

        column_names = [var['Label'] for var in variables]

        df = pd.DataFrame(data, columns=column_names)

        column_rename_map = {
            'Area': 'Area',
            'Periode': 'Periode jobindsats',
            'Ydelsesgrupper': 'Ydelsesgrupper',
            'Forventet og faktisk antal fuldtidspersoner på offentlig forsørgelse: '
            'Forventet antal': 'Forventet antal',
            'Forventet og faktisk antal fuldtidspersoner på offentlig forsørgelse: '
            'Faktisk antal': 'Faktisk antal',
            'Forventet og faktisk antal fuldtidspersoner på offentlig forsørgelse: '
            'Forskel mellem forventet og faktisk antal': 'Forskel mellem forventet '
            'og faktisk antal',
            'Forventet og faktisk andel fuldtidspersoner på offentlig forsørgelse: '
            'Forventet andel (pct.)': 'Forventet andel (pct.)',
            'Forventet og faktisk andel fuldtidspersoner på offentlig forsørgelse: '
            'Faktisk andel (pct.)': 'Faktisk andel (pct.)',
            'Forventet og faktisk andel fuldtidspersoner på offentlig forsørgelse: '
            'Forskel mellem forventet og faktisk andel (pct. point)': 'Forskel '
            'mellem forventet og faktisk andel (pct. point)',
            'Placering på benchmarkranglisten': 'Placering på benchmarkranglisten'
        }

        df = df.rename(columns=column_rename_map)

        df['Periode'] = df['Periode jobindsats']

        df['Periode jobindsats'] = df['Periode jobindsats'].apply(convert_to_datetime)

        df_filtered = df[[
            'Area', 'Periode', 'Periode jobindsats', 'Ydelsesgrupper',
            'Forventet antal', 'Faktisk antal', 'Forskel mellem forventet og '
            'faktisk antal', 'Forventet andel (pct.)', 'Faktisk andel (pct.)',
            'Forskel mellem forventet og faktisk andel (pct. point)',
            'Placering på benchmarkranglisten'
        ]]

        # Convert to csv, set filename and post to CDC
        file = io.BytesIO(df_filtered.to_csv(index=False, sep=';').encode('utf-8'))
        filename = "SA" + "JobindsatsY30R21" + ".csv"

    except Exception as e:
        logger.error(f'Error {e}')
        return False

    if post_data_to_custom_data_connector(filename, file):
        logger.info("Successfully updated JobindsatsY30R21")
        return True
    else:
        logger.error("Failed to update JobindsatsY30R21")
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
        'QMAT02': 4,  # April Q2
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
