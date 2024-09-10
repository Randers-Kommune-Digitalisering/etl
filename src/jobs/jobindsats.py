import io
import json
import logging
from datetime import datetime
import pandas as pd
import requests
from utils.config import JOBINDSATS_API_KEY
from custom_data_connector import post_data_to_custom_data_connector

logger = logging.getLogger(__name__)
filename = 'Jobindsats.csv'


def job():
    logger.info('Starting jobindsats job')

    try:
        tables = api_request('https://api.jobindsats.dk/v2/data/y30r21/json')
        logger.info(tables)

        file = io.BytesIO(tables.to_csv(index=False, sep=';').encode('utf-8'))

        post_data_to_custom_data_connector(filename, file)

    except Exception as e:
        logger.error(f'Error: {e}')
        return False
    else:
        return True


def api_request(endpoint):
    period = dynamic_period()
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
    headers = {
        'Authorization': JOBINDSATS_API_KEY,
    }
    response = requests.post(endpoint, headers=headers, data=json.dumps(payload))
    json_data = response.json()

    variables = json_data[0]['Variables']
    data = json_data[0]['Data']

    column_names = [var['Label'] for var in variables]

    df = pd.DataFrame(data, columns=column_names)

    # Create a dict to shorten the column names
    column_rename_map = {
        'Area': 'Area',
        'Periode': 'Periode jobindsats',
        'Ydelsesgrupper': 'Ydelsesgrupper',
        'Forventet og faktisk antal fuldtidspersoner på offentlig forsørgelse: Forventet antal': 'Forventet antal',
        'Forventet og faktisk antal fuldtidspersoner på offentlig forsørgelse: Faktisk antal': 'Faktisk antal',
        'Forventet og faktisk antal fuldtidspersoner på offentlig forsørgelse: Forskel mellem forventet og faktisk antal': 'Forskel mellem forventet og faktisk antal',
        'Forventet og faktisk andel fuldtidspersoner på offentlig forsørgelse: Forventet andel (pct.)': 'Forventet andel (pct.)',
        'Forventet og faktisk andel fuldtidspersoner på offentlig forsørgelse: Faktisk andel (pct.)': 'Faktisk andel (pct.)',
        'Forventet og faktisk andel fuldtidspersoner på offentlig forsørgelse: Forskel mellem forventet og faktisk andel (pct. point)': 'Forskel mellem forventet og faktisk andel (pct. point)',
        'Placering på benchmarkranglisten': 'BenchmarkRanking'
    }

    df = df.rename(columns=column_rename_map)

    df_filtered = df[[
        'Area', 'Periode jobindsats', 'Ydelsesgrupper', 'Forventet antal',
        'Faktisk antal', 'Forskel mellem forventet og faktisk antal',
        'Forventet andel (pct.)', 'Faktisk andel (pct.)',
        'Forskel mellem forventet og faktisk andel (pct. point)',
        'BenchmarkRanking'
    ]]

    return df_filtered


def dynamic_period():
    current_year = datetime.now().year - 1  # -1 because the data is from the previous year
    previous_year = current_year - 1
    period = [
        f"{previous_year}QMAT02", f"{previous_year}QMAT04",
        f"{current_year}QMAT02", f"{current_year}QMAT04"
    ]
    return period


def df_to_csv(df, filename):
    df.to_csv(f"{filename}.csv", index=False)