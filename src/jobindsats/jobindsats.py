import logging
from datetime import datetime
import pandas as pd
from utils.api_requests import APIClient
from utils.config import JOBINDSATS_API_KEY, JOBINDSATS_URL
from utils.database_connection import get_jobindsats_db

logger = logging.getLogger(__name__)

base_url = JOBINDSATS_URL
jobindsats_client = APIClient(base_url, JOBINDSATS_API_KEY)

db_client = get_jobindsats_db()


def get_data(name, years_back, dataset, period_format, data_to_get):
    try:
        logger.info(f"Starting jobindsats: {name}")
        latest_period = period_request(dataset, period_format)
        if not latest_period:
            logger.error("Failed to get the latest period")
            return False

        period = dynamic_period(latest_period, years_back, period_format)
        if not period:
            logger.error("Failed to generate periods")
            return False

        payload = {"area": "*", "period": period} | data_to_get

        data = jobindsats_client.make_request(path=f'v2/data/{dataset}/json', json=payload)

        variables = data[0]['Variables']
        data = data[0]['Data']

        column_names = [var['Label'] for var in variables]

        df = pd.DataFrame(data, columns=column_names)

        df[f'Periode {name}'] = df['Periode'].apply(convert_to_datetime)

        output_table = f"jobindsats_{dataset.replace('_', '').lower()}"

        db_client.ensure_database_exists()
        connection = db_client.get_connection()
        if connection:
            df.to_sql(output_table, con=connection, if_exists='replace', index=False)
            logger.info(f"Successfully saved {output_table} to the database")
            connection.close()
            return True
        else:
            logger.error("Failed to get database connection")
            return False

    except Exception as e:
        logger.error(f'Error {e}')
        return False


def dynamic_period(latest_period, years_back, period_format):
    try:
        period = []
        if period_format == 'QMAT' and 'QMAT' in latest_period:
            current_year = int(latest_period[:4])
            current_qmat = int(latest_period[8:])

            for qmat in range(1, current_qmat + 1):
                period.append(f"{current_year}QMAT{qmat:02d}")

            if years_back:
                for year in range(current_year - years_back, current_year):
                    for qmat in range(1, 13):
                        period.append(f"{year}QMAT{qmat:02d}")

        elif period_format == 'Q' and 'Q' in latest_period:
            current_year = int(latest_period[:4])
            current_quarter = int(latest_period[5:])

            for quarter in range(1, current_quarter + 1):
                period.append(f"{current_year}Q{quarter}")

            if years_back:
                for year in range(current_year - years_back, current_year):
                    for quarter in range(1, 5):
                        period.append(f"{year}Q{quarter}")

        elif period_format == 'M' and 'M' in latest_period:
            current_year = int(latest_period[:4])
            current_month = int(latest_period[5:])

            for month in range(1, current_month + 1):
                period.append(f"{current_year}M{month:02d}")

            if years_back:
                for year in range(current_year - years_back, current_year):
                    for month in range(1, 13):
                        period.append(f"{year}M{month:02d}")

        return period
    except Exception as e:
        logger.error(f'Error in dynamic_period: {e}')
        return []


def convert_to_datetime(period_str):
    year = int(period_str[:4])
    if 'QMAT' in period_str:
        qmat = int(period_str[8:])
        month = qmat
    elif 'Q' in period_str:
        quarter = int(period_str[5:])
        month = (quarter - 1) * 3 + 1
    else:
        month = int(period_str[5:])
    return datetime(year, month, 1)


def period_request(dataset, period_format):
    try:
        data = jobindsats_client.make_request(path=f'v2/tables/{dataset}/json/')
        periods = data[0]['Period']

        if period_format == 'QMAT':
            valid_periods = [p for p in periods if len(p) == 10 and p[4:8] == 'QMAT' and p[8:].isdigit()]
        elif period_format == 'Q':
            valid_periods = [p for p in periods if len(p) == 6 and p[4] == 'Q' and p[5:].isdigit()]
        elif period_format == 'M':
            valid_periods = [p for p in periods if len(p) == 7 and p[4] == 'M' and p[5:].isdigit()]
        else:
            valid_periods = []

        if not valid_periods:
            logger.error("No valid periods found")
            return None

        latest_period = max(valid_periods)
        return latest_period
    except Exception as e:
        logger.error(f'Error fetching period for dataset {dataset}: {e}')
        raise
