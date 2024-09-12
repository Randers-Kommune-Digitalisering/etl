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



def get_jobindats_ydelsesgrupper():
    try:
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
        data = jobindsats_client.make_request(json=payload)

        variables = data[0]['Variables']
        data = data[0]['Data']

        column_names = [var['Label'] for var in variables]

        df = pd.DataFrame(data, columns=column_names)

        # Create a dict to shorten the column names
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

        # Add the original period string as a new column
        df['Periode'] = df['Periode jobindsats']

        # Convert 'Periode jobindsats' to datetime instead of being a string
        df['Periode jobindsats'] = df['Periode jobindsats'].apply(convert_to_datetime)

        # Filter columns for the Variables'
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


def dynamic_period():
    current_year = datetime.now().year - 1  # -1 Because the dataset[y30r21] is from 2023
    previous_year = current_year - 1
    period = [
        f"{previous_year}QMAT02", f"{previous_year}QMAT04",
        f"{current_year}QMAT02", f"{current_year}QMAT04"
    ]
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
    