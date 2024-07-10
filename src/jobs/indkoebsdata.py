# Indkøbsdata fra Truelink / Mercell

import shutil
import io
import zipfile
import logging
import fnmatch
import pandas as pd

from enum import Enum
from pathlib import Path
from sqlalchemy import text
from numpy.dtypes import ObjectDType

from database import get_database_connection
from utils.config import TRUELINK_SFTP_HOST, TRUELINK_SFTP_USER, TRUELINK_SSH_KEY_BASE64, TRUELINK_SFTP_REMOTE_DIR
from utils.stfp import SFTPClient
from custom_data_connector import post_data_to_custom_data_connector
from utils.utils import format_text


logger = logging.getLogger(__name__)


class Routes(Enum):
    CLIMATE_DB = 1
    BI_SYS = 2


class Types(Enum):
    FUEL = {'keyword': 'Brændstof', 'route': Routes.CLIMATE_DB, 'prefix': 'truelink'}
    ECOMM = {'keyword': 'E-handelsfilter', 'route': Routes.BI_SYS, 'prefix': 'ØK'}


def job():
    sftp_client = SFTPClient(TRUELINK_SFTP_HOST, TRUELINK_SFTP_USER, key_base64=TRUELINK_SSH_KEY_BASE64)
    conn = sftp_client.get_connection()
    filelist = [f for f in conn.listdir(TRUELINK_SFTP_REMOTE_DIR) if fnmatch.fnmatch(f, '*.*')] if conn else None
    if filelist and conn:
        return route_files(filelist, conn)
    return False


def route_files(filelist, connection):
    status = []
    track_types = []
    for t in list(Types):
        t.value['files'] = []
        track_types.append(t.value)

    for f in filelist:
        for t in Types:
            if t.value['keyword'] in f:
                current_tt = next(tt for tt in track_types if tt['keyword'] == t.value['keyword'])
                current_tt['files'].append(f)

    for tt in track_types:
        match tt['route']:
            case Routes.CLIMATE_DB:
                status.append(handle_climate_db(tt['files'], connection, tt['prefix'], tt['keyword']))
                pass
            case Routes.BI_SYS:
                status.append(handle_bi_sys(tt['files'], connection, tt['prefix']))
                pass
            case _:
                raise TypeError('Unknown route')

    return all(status)


def handle_bi_sys(files, connection, prefix):
    status = []
    for filename in files:
        file = connection.open(filename)
        zip = zipfile.ZipFile(file, 'r')

        # Assume just ONE csv file
        csv_file = [f for f in zip.namelist() if f.endswith(('.csv'))][0]
        if 'test' in csv_file:
            with zip.open(csv_file, 'r') as f:
                all_lines = f.readlines()

            for i in range(len(all_lines)):
                line = all_lines[i].decode('cp1252')

                # Strips leading equal signs from all lines
                if (line[0] == '='):
                    line = line[1:]
                line = line.replace(';=', ';')
                line = line.replace('"', '')

                # Adds 'n/a' to empty columns in first row - assumes empty columns should be strings
                if i == 1:
                    first_line_arr = line.split(';')
                    line = ';'.join(['"n/a"' if not e.strip() else e for e in first_line_arr]) + '\n'

                all_lines[i] = line.strip()

            with io.BytesIO() as outfile:
                encoded_outfile = io.TextIOWrapper(outfile, 'utf-8', newline='')
                encoded_outfile.write('\n'.join(all_lines))

                new_filename = Path(filename).stem.replace(' ', '_')

                ok = post_data_to_custom_data_connector(prefix + new_filename, outfile.getbuffer())
                status.append(ok)

                if ok:
                    logger.info(f'Updated {prefix + new_filename}')
                else:
                    logger.error(f'Failed to update {prefix + new_filename}')
        else:
            logger.info(f'File {csv_file} is old format, skipping')

    return all(status)


def handle_climate_db(files, connection, prefix, keyword):
    with io.BytesIO() as outfile:
        for i, filename in enumerate(files):

            file = connection.open(filename)
            zip = zipfile.ZipFile(file, 'r')

            # Assume just ONE csv file
            csv_file = [f for f in zip.namelist() if f.endswith(('.csv'))][0]
            with zip.open(csv_file, 'r') as f:
                if i != 0:
                    f.readline()
                shutil.copyfileobj(f, outfile)

        outfile.seek(0)
        decoded_string = io.StringIO(outfile.read().decode('cp1252'))
        data = pd.read_csv(decoded_string, sep=';')
        df = pd.DataFrame(data)
        df.columns = [format_text(c) for c in list(df)]
        for col in df.columns:
            if df.dtypes[col] == ObjectDType:
                try:
                    df[col] = pd.to_datetime(df[col])
                except Exception:
                    pass

                try:
                    df[col] = pd.to_numeric(df[col].str.replace(',', '.'))
                except Exception:
                    pass

        df['opdateret'] = pd.Timestamp.now()
        df.index.names = ['id']

        table_name = format_text(prefix + '_' + keyword)

        try:
            with get_database_connection('climate') as conn:
                df.to_sql(table_name, con=conn, if_exists='replace')
                conn.execute(text(f'ALTER TABLE `{table_name}` ADD PRIMARY KEY (`id`);'))
        except Exception as e:
            logger.error(f'Failed to update {keyword}', e)
            return False

    logger.info(f'Updated {keyword}')
    return True
