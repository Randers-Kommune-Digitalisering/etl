# Ydelsesrefusion fra serviceplatformen
import os
import io
import fnmatch
import datetime
import logging
import pandas as pd

from datetime import timedelta

from utils.config import SERVICEPLATFORM_SFTP_REMOTE_DIR, SERVICEPLATFORM_SFTP_HOST, SERVICEPLATFORM_SFTP_USER, SERVICEPLATFORM_SSH_KEY_BASE64, SERVICEPLATFORM_SSH_KEY_PASS
from utils.stfp import SFTPClient
from custom_data_connector import post_data_to_custom_data_connector


logger = logging.getLogger(__name__)


def job():
    sftp_client = SFTPClient(SERVICEPLATFORM_SFTP_HOST, SERVICEPLATFORM_SFTP_USER, key_base64=SERVICEPLATFORM_SSH_KEY_BASE64, key_pass=SERVICEPLATFORM_SSH_KEY_PASS)
    conn = sftp_client.get_connection()
    filelist = [f for f in conn.listdir(SERVICEPLATFORM_SFTP_REMOTE_DIR) if fnmatch.fnmatch(f, '*.*')] if conn else None
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

        logger.info(f'Data periode: {min_date} - {max_date}')

        # Filter files based on min date
        files = [f for f in files if datetime.date(int(f[-14:-10]), int(f[-9:-7]), 1) >= min_date]

        df_list = []

        # Covnert min_date to dtype datetime64
        min_date = pd.to_datetime(min_date)

        for filename in files:
            with connection.open(os.path.join(SERVICEPLATFORM_SFTP_REMOTE_DIR, filename).replace("\\", "/")) as f:
                # Read needed columns from csv
                needed_cols = ['Uge', 'CPR nummer', 'Ydelse', 'Finansiering Kommunenavn', 'Beregnet udbetalingsbeløb', 'Refusionssats', 'Refusionsbeløb', 'Medfinansieringssats', 'Medfinansieringsbeløb' ]
                df = pd.read_csv(f, sep=";", header=0, decimal=",", na_filter=False, parse_dates=['Uge'], date_format='%Y-%m-%d', usecols=needed_cols)

                # Remove rows where 'Ydelse' == 'Fleksbidrag fra staten' TODO: Check if this is correct
                # and drop duplicates
                df = df[df['Ydelse'] != 'Fleksbidrag fra staten'].drop_duplicates()

                # Filter rows based on min date
                df = df[df['Uge'] >= min_date]

                df_list.append(df)

        df = pd.concat(df_list, ignore_index=True)

        # Sum 'Refusionsbeløb' and 'Medfinansieringsbeløb' for each 'Uge', 'CPR nummer' and 'Ydelse.'
        df = df.groupby(['Uge', 'CPR nummer', 'Ydelse', 'Finansiering Kommunenavn'])[['Beregnet udbetalingsbeløb', 'Refusionsbeløb', 'Medfinansieringsbeløb']].sum().reset_index()

        # Drop all rows where 'Beregnet udbetalingsbeløb' == 0
        df = df[df['Beregnet udbetalingsbeløb'] != 0]
        
        # Data på individniveau
        df_alt = df.copy()  

        # Grupperer data
        df = df.groupby(['Uge', 'Ydelse', 'Finansiering Kommunenavn']).agg(Antal=('CPR nummer', 'count'), Total=('Beregnet udbetalingsbeløb', 'sum'), Refusion=('Refusionsbeløb', 'sum'), Medfinansiering=('Medfinansieringsbeløb', 'sum')).reset_index()

        # Round to 2 decimal places
        colums = ['Total', 'Refusion', 'Medfinansiering']
        df[colums] = df[colums].round(2)

        # Convert to csv, set filename and post to custom data connector
        file = io.BytesIO(df.to_csv(index=False, sep=';').encode('utf-8'))
        filename = "SA" + "Ydelsesrefusion" + ".csv"

        # Round to 2 decimal places
        colums_alt = ['Beregnet udbetalingsbeløb', 'Refusionsbeløb', 'Medfinansieringsbeløb']
        df_alt[colums] = df_alt[colums_alt].round(2)

        # Convert to csv, set filename and post to custom data connector
        file_alt = io.BytesIO(df.to_csv(index=False, sep=';').encode('utf-8'))
        filename_alt = "SA" + "YdelsesrefusionIndivid" + ".csv"

    except Exception as e:
        logger.error(e)
        return False

    if post_data_to_custom_data_connector(filename, file) and post_data_to_custom_data_connector(filename_alt, file_alt):
        logger.info("Updated Ydelsesrefusion")
        return True
    else:
        logger.error("Failed to update Ydelsesrefusion")
        return False
