# Ydelsesrefusion fra serviceplatformen
import os
import warnings
import io
import fnmatch
import datetime
import logging
import pandas as pd

from datetime import timedelta

from utils.config import SERVICEPLATFORM_SFTP_REMOTE_DIR
from utils.stfp import get_filelist_and_connection
from custom_data_connector import post_data_to_custom_data_connector


logger = logging.getLogger(__name__)
warnings.filterwarnings('ignore', '.*Failed to load HostKeys.*')


def job():
    filelist, conn = get_filelist_and_connection('SERVICEPLATFORM')
    if filelist and conn:
        return handle_files(filelist, conn)
    return False


def handle_files(files, connection):
    logger.info('Handling Ydelsesrefusion files')

    try:
        # Filter files based on filename
        files = [f for f in files if fnmatch.fnmatch(f, 'yr-ydelsesrefusion-beregning*.csv')]

        # Get min and max date based on last filename
        date = datetime.date(int(files[-1][-14:-10]), int(files[-1][-9:-7]), 1)
        max_date = date - timedelta(days=1)
        min_date = datetime.date(date.year - 2, (date.month % 12) + 1, date.day)

        logger.info(f'Data peridoe: {min_date} - {max_date}')

        # Filter files based on min date
        files = [f for f in files if datetime.date(int(f[-14:-10]), int(f[-9:-7]), 1) >= min_date]

        df_list = []

        # Covnert min_date to dtype datetime64
        min_date = pd.to_datetime(min_date)

        for filename in files:
            with connection.open(os.path.join(SERVICEPLATFORM_SFTP_REMOTE_DIR, filename).replace("\\", "/")) as f:
                # Read needed columns from csv
                needed_cols = ['Uge', 'CPR nummer', 'Ydelse', 'Beregnet udbetalingsbeløb', 'Refusionssats', 'Refusionsbeløb', 'Medfinansieringssats', 'Medfinansieringsbeløb']
                df = pd.read_csv(f, sep=";", header=0, decimal=",", na_filter=False, parse_dates=['Uge'], date_format='%Y-%m-%d', usecols=needed_cols)

                # Remove rows where 'Ydelse' == 'Fleksbidrag fra staten' TODO: Check if this is correct
                # and drop duplicates
                df = df[df['Ydelse'] != 'Fleksbidrag fra staten'].drop_duplicates()

                # Filter rows based on min date
                df = df[df['Uge'] >= min_date]

                df_list.append(df)

        df = pd.concat(df_list, ignore_index=True)

        # Sum 'Refusionsbeløb' and 'Medfinansieringsbeløb' for each 'Uge', 'CPR nummer' and 'Ydelse.'
        df = df.groupby(['Uge', 'CPR nummer', 'Ydelse'])[['Beregnet udbetalingsbeløb', 'Refusionsbeløb', 'Medfinansieringsbeløb']].sum().reset_index()

        # Drop all rows where 'Beregnet udbetalingsbeløb' == 0
        df = df[df['Beregnet udbetalingsbeløb'] != 0]

        df = df.groupby(['Uge', 'Ydelse']).agg(Antal=('CPR nummer', 'count'), Total=('Beregnet udbetalingsbeløb', 'sum'), Refusion=('Refusionsbeløb', 'sum'), Medfinansiering=('Medfinansieringsbeløb', 'sum')).reset_index()

        # Round to 2 decimal places
        colums = ['Total', 'Refusion', 'Medfinansiering']
        df[colums] = df[colums].round(2)

        # Convert to csv, set filename and post to custom data connector
        file = io.BytesIO(df.to_csv(index=False, sep=';').encode('utf-8'))
        filename = "SA" + "Ydelsesrefusion" + ".csv"

    except Exception as e:
        logger.error(e)
        return False

    if post_data_to_custom_data_connector(filename, file):
        logger.info("Updated Ydelsesrefusion")
        return True
    else:
        logger.error("Failed to update Ydelsesrefusion")
        return False
