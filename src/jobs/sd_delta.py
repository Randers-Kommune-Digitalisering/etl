import pasnd as pd

from config.config import SD_DELTA_EXCLUDED_DEPARTMENTS_FILE_PATH

EXCLUDED_DEPARTMENTS_DF = pd.read_csv(SD_DELTA_EXCLUDED_DEPARTMENTS_FILE_PATH, sep=';', encoding='cp1252')


def job():
    return True
