import logging
import io
import pandas as pd


logger = logging.getLogger(__name__)


def format_text(string):
    trans_dict = dict.fromkeys(' -/', '_')
    trans_dict.update({'ø': 'oe', 'å': 'aa', 'æ': 'ae'})
    return string.lower().translate(str.maketrans(trans_dict))


def flatten_xml(element):
    result = {}
    for child in element.iter():
        if child.tag != element.tag:
            key = child.tag
            value = child.text.strip() if child.text else None
            result[key] = value
            result.update(flatten_xml(child))
    return result


def df_to_excel_bytes(df: pd.DataFrame):
    excel_file = io.BytesIO()

    with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)

    excel_file.seek(0)

    return excel_file


def df_to_csv_bytes(df: pd.DataFrame, sep: str = ';', encoding: str = 'UTF-8'):
    csv_file = io.BytesIO()

    df.to_csv(csv_file, index=False, sep=sep, encoding=encoding)

    csv_file.seek(0)

    return csv_file
