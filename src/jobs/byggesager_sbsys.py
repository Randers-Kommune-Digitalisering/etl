import io
import csv
import fnmatch
import urllib.parse
import pandas as pd

from datetime import datetime, timedelta

from utils.config import UMT_SBSYS_SFTP_HOST, UMT_SBSYS_SFTP_USER, UMT_SBSYS_SFTP_PASS, CONFIG_LIBRARY_URL, CONFIG_LIBRARY_USER, CONFIG_LIBRARY_PASS, CONFIG_LIBRARY_BASE_PATH, BYGGESAGER_CONFIG_FILE
from utils.stfp import SFTPClient
from utils.api_requests import APIClient
from utils.logging import logging
from custom_data_connector import read_data_from_custom_data_connector, post_data_to_custom_data_connector


logger = logging.getLogger(__name__)

config_library_client = APIClient(base_url=CONFIG_LIBRARY_URL, username=CONFIG_LIBRARY_USER, password=CONFIG_LIBRARY_PASS)


def job():
    config_path = urllib.parse.urljoin(CONFIG_LIBRARY_BASE_PATH, BYGGESAGER_CONFIG_FILE)
    config = config_library_client.make_request(path=config_path)
    if not config:
        logging.error(f"Failed to load config file: {BYGGESAGER_CONFIG_FILE}")
        return False

    last_month = (datetime.now().replace(day=1) - timedelta(days=1)).replace(day=1)
    month_before_last = (last_month - timedelta(days=1)).replace(day=1)

    last_month = last_month.strftime('%d-%m-%Y')
    month_before_last = month_before_last.strftime('%d-%m-%Y')

    files_updated = []
    for file_to_update in config['files_to_update']:
        old_file = read_data_from_custom_data_connector(file_to_update, path='in')
        if check_up_to_date(old_file, [last_month, month_before_last]):
            logger.info(f'{old_file.filename} already up to date')
            files_updated.append(True)

    if all(files_updated) and len(files_updated) == len(config['files_to_update']):
        logger.info('All files already up to date')
        return True

    new_data, update_data = get_sbsys_data(config)

    if new_data and update_data:
        results = []
        for file_to_update in config['files_to_update']:
            if old_file:
                for key in new_data:
                    old_file = read_data_from_custom_data_connector(file_to_update, path='in')
                    if key in old_file.filename:
                        if update_data.get(key, {}).get('file', None) and new_data.get(key, {}).get('file', None):
                            update_date = update_data.get(key, {}).get('date', None)
                            if update_date:
                                updated_file = update_csv_file(old_file, update_data[key]['file'], update_date)

                                new_date = new_data.get(key, {}).get('date', None)
                                if new_date:
                                    if check_csv_file_for_string(updated_file, new_date):
                                        logger.info(f'{old_file.filename} already up to date - skipping')
                                        results.append(True)
                                        continue

                                    new_file = combine_two_csv_files(updated_file, new_data[key]['file'])
                                    if post_data_to_custom_data_connector(key, new_file):
                                        logger.info(f'{key} uploaded to custom-data-connector - {file_to_update} updated')
                                        results.append(True)
                                        continue
                                    else:
                                        logger.warning(f'{key} failed to upload to custom-data-connector - {file_to_update} NOT updated')
                                else:
                                    logger.warning('No new date provided')
                                    results.append(False)
                                    continue
                            else:
                                logger.warning('No update date provided')
                                results.append(False)
                                continue
            elif old_file is None:
                for key in new_data:
                    if key in file_to_update:
                        logger.info(f'{file_to_update} does not exist in custom-data-connector - creating')
                        if new_data.get(key, {}).get('file', None) and update_data.get(key, {}).get('date', None):
                            file = combine_two_csv_files(update_data[key]['file'], new_data[key]['file'],)
                            if post_data_to_custom_data_connector(key, file):
                                logger.info(f'{key} uploaded to custom-data-connector - {file_to_update} created')
                                results.append(True)
                                continue
                            else:
                                logger.warning(f'{key} failed to upload to custom-data-connector - {file_to_update} NOT created')
                                results.append(False)
                                continue
                        else:
                            logger.warning(f'Missing file for {key} - {file_to_update} NOT created')
                            results.append(False)
                            continue
            else:
                logger.error('Custom-data-connector failed')
                return False
    else:
        logger.error('UMT SBSYS SFTP failed')
        return False

    return all(results)


# Helper functions
def get_sbsys_data(config):
    try:
        last_modified_received = []
        last_modified_concluded = []
        # received_df = pd.DataFrame(columns=['Byggesagskode', 'Antal', 'År'])
        # concluded_df = pd.DataFrame(columns=['Byggesagskode', 'Beslutningstype', 'Antal', 'År'])
        received_df_new_original = pd.DataFrame(columns=['Byggesagskode', 'LastMonth', 'MonthBeforeLast', 'Year'])
        concluded_df_new_original = pd.DataFrame(columns=['Byggesagskode', 'Beslutningstype', 'LastMonth', 'MonthBeforeLast', 'Year'])

        def read_file(file):
            df = pd.read_csv(file, sep=';', header=None)
            df = df.astype(str)
            return df

        with SFTPClient(UMT_SBSYS_SFTP_HOST, UMT_SBSYS_SFTP_USER, password=UMT_SBSYS_SFTP_PASS).get_connection() as conn:
            filelist = [f for f in conn.listdir() if fnmatch.fnmatch(f, '*.csv')] if conn else None

            for file_name in filelist:
                if all(x in file_name.lower() for x in [config['keywords']['received'], config['keywords']['template_code']]):
                    last_modified_received.append(datetime.fromtimestamp(conn.stat(file_name).st_mtime))
                    with conn.open(file_name, 'r') as file:
                        df = read_file(file)
                        df.columns = received_df_new_original.columns
                        received_df_new_original = pd.concat([received_df_new_original, df], axis=0)

                elif all(x in file_name.lower() for x in [config['keywords']['received'], config['keywords']['building_code']]):
                    last_modified_received.append(datetime.fromtimestamp(conn.stat(file_name).st_mtime))
                    with conn.open(file_name, 'r') as file:
                        df = read_file(file)
                        df.columns = received_df_new_original.columns
                        received_df_new_original = pd.concat([received_df_new_original, df], axis=0)

                elif all(x in file_name.lower() for x in [config['keywords']['concluded'], config['keywords']['template_code']]):
                    last_modified_concluded.append(datetime.fromtimestamp(conn.stat(file_name).st_mtime))
                    with conn.open(file_name, 'r') as file:
                        df = read_file(file)
                        df.columns = concluded_df_new_original.columns
                        concluded_df_new_original = pd.concat([concluded_df_new_original, df], axis=0)

                elif all(x in file_name.lower() for x in [config['keywords']['concluded'], config['keywords']['building_code']]):
                    last_modified_concluded.append(datetime.fromtimestamp(conn.stat(file_name).st_mtime))
                    with conn.open(file_name, 'r') as file:
                        df = read_file(file)
                        df.columns = concluded_df_new_original.columns
                        concluded_df_new_original = pd.concat([concluded_df_new_original, df], axis=0)

        now = datetime.now()
        if all(date.month == now.month and date.year == now.year for date in last_modified_received + last_modified_concluded):
            dates_received = [(dt.replace(day=1) - timedelta(days=1)).replace(day=1).strftime('%d-%m-%Y') for dt in last_modified_received]
            dates_concluded = [(dt.replace(day=1) - timedelta(days=1)).replace(day=1).strftime('%d-%m-%Y') for dt in last_modified_concluded]

            received_dates_match = all(date == dates_received[0] for date in dates_received) if dates_received else False
            concluded_dates_match = all(date == dates_concluded[0] for date in dates_concluded) if dates_concluded else False

            received_filename = "UMTByggesagerModtagede.csv"
            concluded_filename = "UMTByggesagerAfgjorte.csv"

            received_file_new = None
            concluded_file_new = None

            update_month_received_date = (datetime.strptime(dates_received[0], '%d-%m-%Y') - timedelta(days=1)).replace(day=1).strftime("%d-%m-%Y")
            update_month_concluded_date = (datetime.strptime(dates_concluded[0], '%d-%m-%Y') - timedelta(days=1)).replace(day=1).strftime("%d-%m-%Y")

            if received_dates_match:
                received_df_new = clean_dataframe(received_df_new_original, dates_received[0], config['code_mapping'], config['template_name_mapping'], config['allowed_decision_types'], 'LastMonth', 'MonthBeforeLast')
                received_df_update = clean_dataframe(received_df_new_original, update_month_received_date, config['code_mapping'], config['template_name_mapping'], config['allowed_decision_types'], 'MonthBeforeLast', 'LastMonth')
                received_file_new = io.BytesIO(received_df_new.to_csv(index=False, sep=';').encode('utf-8'))
                received_file_new.seek(0)
                received_file_update = io.BytesIO(received_df_update.to_csv(index=False, sep=';').encode('utf-8'))
                received_file_update.seek(0)
            else:
                logger.warning('Received dates do not match')

            if concluded_dates_match:
                concluded_df_new = clean_dataframe(concluded_df_new_original, dates_concluded[0], config['code_mapping'], config['template_name_mapping'], config['allowed_decision_types'], 'LastMonth', 'MonthBeforeLast')
                concluded_df_update = clean_dataframe(concluded_df_new_original, update_month_concluded_date, config['code_mapping'], config['template_name_mapping'], config['allowed_decision_types'], 'MonthBeforeLast', 'LastMonth')
                concluded_file_new = io.BytesIO(concluded_df_new.to_csv(index=False, sep=';').encode('utf-8'))
                concluded_file_new.seek(0)
                concluded_file_update = io.BytesIO(concluded_df_update.to_csv(index=False, sep=';').encode('utf-8'))
                concluded_file_update.seek(0)

            else:
                logger.warning('Concluded dates do not match')

            return {received_filename: {"file": received_file_new, "date": dates_received[0]}, concluded_filename: {"file": concluded_file_new, "date": dates_concluded[0]}}, \
                {received_filename: {"file": received_file_update, "date": update_month_received_date}, concluded_filename: {"file": concluded_file_update, "date": update_month_concluded_date}}
        else:
            logger.error('Files not updated this month!')
    except Exception as e:
        logger.error(e)


def map_codes(value):
    if 'x' not in value:
        if len(value.split('-')) > 2:
            return value.split(' ')[0]
        else:
            return value.split('-')[0].strip().upper().replace(' ', '-')
    return value.split(',')[0].strip().upper().replace(' - ', '-').replace(' ', '-')


def custom_sort_key(value):
    return (1, value) if value.strip().isdigit() else (0, value)


def clean_dataframe(df, date, code_mapping, template_name_mapping, allowed_decision_types, month_to_use, month_to_drop):
    df = df.copy(deep=True)

    # Add grouping
    df['codes'] = df['Byggesagskode'].apply(map_codes)
    reverse_mapping = {k: v for v, keys in code_mapping.items() for k in keys}
    df.insert(1, 'Gruppering', df['codes'].map(reverse_mapping))

    # Clenup
    df.insert(0, 'Dato', date)
    df = df.dropna(subset=['Gruppering'], how='any')
    df['Antal'] = df[month_to_use].astype(int)

    df = df.drop(['Year', 'codes', month_to_use, month_to_drop], axis=1)

    if 'Beslutningstype' in df.columns:
        # Fix incorrect decision types
        df['Beslutningstype'] = df['Beslutningstype'].fillna('Besvaret')
        df['Beslutningstype'] = df['Beslutningstype'].apply(lambda x: x if x in allowed_decision_types else 'Besvaret')

        # Add rows for non-value decision types
        for byggesagskode in df['Byggesagskode'].unique():
            for decision_type in allowed_decision_types:
                if not ((df['Byggesagskode'] == byggesagskode) & (df['Beslutningstype'] == decision_type)).any():
                    df = pd.concat([df, pd.DataFrame([{'Dato': date, 'Byggesagskode': byggesagskode, 'Gruppering': reverse_mapping.get(map_codes(byggesagskode), ''), 'Beslutningstype': decision_type, 'Antal': 0}])], ignore_index=True)

    # Renaming
    df['Byggesagskode'] = df['Byggesagskode'].str.replace(r'\s+', ' ', regex=True)
    df['Byggesagskode'] = df['Byggesagskode'].apply(lambda x: template_name_mapping.get(x, x) if x.isdigit() else x)

    if 'Beslutningstype' in df.columns:
        df = df.groupby(['Dato', 'Byggesagskode', 'Gruppering', 'Beslutningstype'], as_index=False).agg({'Antal': 'sum'})
        df = df.sort_values(by=['Byggesagskode', 'Beslutningstype'], key=lambda col: col.map(custom_sort_key))
    else:
        df = df.groupby(['Dato', 'Byggesagskode', 'Gruppering'], as_index=False).agg({'Antal': 'sum'})
        df = df.sort_values(by='Byggesagskode', key=lambda col: col.map(custom_sort_key))

    return df


def check_csv_file_for_string(csv_file, string_to_check):
    reader = csv.reader(io.StringIO(csv_file.getvalue().decode('utf-8')), delimiter=';')
    return any(string_to_check in row for row in reader)


def combine_two_csv_files(file1, file2):
    df1 = pd.read_csv(file1, sep=';')
    df2 = pd.read_csv(file2, sep=';')

    if not df1.columns.equals(df2.columns):
        raise ValueError("The two CSV files do not have the same columns")

    df_combined = pd.concat([df1, df2], ignore_index=True)
    file_combined = io.BytesIO(df_combined.to_csv(index=False, sep=';').encode('utf-8'))

    return file_combined


def update_csv_file(old_file, new_file, date):
    df_old = pd.read_csv(old_file, sep=';')
    df_new = pd.read_csv(new_file, sep=';')

    if not df_old.columns.equals(df_new.columns):
        raise ValueError("The two CSV files do not have the same columns")

    df_old.loc[df_old['Dato'] == date] = df_new.values

    updated_file = io.BytesIO(df_old.to_csv(index=False, sep=';').encode('utf-8'))

    return updated_file


def check_up_to_date(old_file, dates):
    all_dates = []
    for string_to_check in dates:
        if check_csv_file_for_string(old_file, string_to_check):
            all_dates.append(True)
            continue
    return all(all_dates)
