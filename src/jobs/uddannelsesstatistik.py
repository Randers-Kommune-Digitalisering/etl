# uddannelsesstatistik.dk API documentation: https://api.uddannelsesstatistik.dk/
import io
import logging
import datetime
import pandas as pd

from utils.config import UDDANNELSESSTATISTIK_URL, UDDANNELSESSTATISTIK_API_KEY
from utils.api_requests import APIClient
from custom_data_connector import post_data_to_custom_data_connector

logger = logging.getLogger(__name__)
udddannels_stattistik_client = APIClient(UDDANNELSESSTATISTIK_URL, api_key=UDDANNELSESSTATISTIK_API_KEY, use_bearer=True)
starting_year = 2022
filename = 'BSelevtrivsel-4til9klasse.csv'


def job():
    return get_well_being_data()


def get_well_being_data():
    try:
        date = datetime.datetime.now()
        years = []
        while date.year > starting_year:
            years.append(f'{date.year - 1}/{date.year}')
            date = date.replace(year=date.year - 1)

        payload = {
            "område": "GS",
            "emne": "TRIV",
            "underemne": "TRIVIND",
            "nøgletal": [
                "Indikatorsvar"
            ],
            "detaljering": [
                "[Institution].[Institution]",
                "[Klassetrin].[Klassetringruppe]",
                "[Skoleår].[Skoleår]",
                "[Trivselsindikator].[Trivselsindikator]"
            ],
            "filtre": {
                "[Institution].[Institution Beliggenhedskommune]": [
                    "Randers"
                ],
                "[Institution].[Institutionstype]": [
                    "Folkeskoler"
                ],
                "[Klassetrin].[Klassetringruppe]": [
                    "Udskoling",
                    "Mellemtrin"
                ],
                "[Skoleår].[Skoleår]": years
            },
            "indlejret": True,
            "tomme_rækker": False,
            "formattering": "json"
        }

        data = udddannels_stattistik_client.make_request(json=payload)
        if data:
            expanded_data = []
            for key_0 in list(data):
                for key_1 in list(data[key_0]):
                    for key_2 in list(data[key_0][key_1]):
                        row = {'År': key_2, 'Skolenavn': key_0, 'Trin': key_1}
                        for key_3 in list(data[key_0][key_1][key_2]):
                            for key_4 in list(data[key_0][key_1][key_2][key_3]):
                                row[key_3] = data[key_0][key_1][key_2][key_3][key_4]
                        expanded_data.append(row)
        df = pd.DataFrame(expanded_data)

        file = io.BytesIO(df.to_csv(index=False, sep=';').encode('utf-8'))

    except Exception as e:
        logger.error(e)
        return False

    if post_data_to_custom_data_connector(filename, file):
        logger.info('Updated "Elevtrivsel 4. til 9. klasse"')
        return True
    else:
        logger.error('Failed to update "Elevtrivsel 4. til 9. klasse"')
        return False
