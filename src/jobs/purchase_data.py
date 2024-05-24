# Indkøbsdata fra Truelink / Mercell

import shutil
import io
import zipfile
import logging
import pandas as pd

from sqlalchemy import text
from numpy.dtypes import ObjectDType

from database import get_database_connection

logger = logging.getLogger(__name__)


def format_text(string):
    trans_dict = dict.fromkeys(' -/', '_')
    trans_dict.update({'ø': 'oe', 'å': 'aa', 'æ': 'ae'})
    return string.lower().translate(str.maketrans(trans_dict))


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

        with get_database_connection('climate') as conn:
            df.to_sql(table_name, con=conn, if_exists='replace')
            conn.execute(text(f'ALTER TABLE `{table_name}` ADD PRIMARY KEY (`id`);'))

    logger.info(f'Updated {keyword}')
