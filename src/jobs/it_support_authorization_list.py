
import datetime
import logging
import pandas as pd

from utils.api_requests import APIClient
from utils.utils import df_to_excel_bytes
from mail import send_mail_with_attachment
from logiva_signflow import LogivaSignflowClient
from delta_client import DeltaClient
from sd_client import SDClient
from utils.config import SD_URL, SD_USER, SD_PASS, LOGIVA_URL, LOGIVA_USER, LOGIVA_PASS, MAIL_SERVER_URL, DELTA_AUTH_URL, DELTA_CLIENT_ID, DELTA_CLIENT_SECRET, DELTA_URL, DELTA_REALM, SD_DELTA_FROM_MAIL, IT_SUPPORT_MAIL


logger = logging.getLogger(__name__)
delta_client = DeltaClient(base_url=DELTA_URL, auth_url=DELTA_AUTH_URL, realm=DELTA_REALM, client_id=DELTA_CLIENT_ID, client_secret=DELTA_CLIENT_SECRET)
sd_client = SDClient(SD_URL, SD_USER, SD_PASS)
ls_client = LogivaSignflowClient(LOGIVA_URL, LOGIVA_USER, LOGIVA_PASS)
mail_client = APIClient(base_url=MAIL_SERVER_URL)


def job():
    logger.info("Starting IT Support Authorization List job...")
    df = ls_client.get_it_department_authorizations_df()

    filtered_signflow_df = df.loc[df['Action'].isin(['Nyansat', 'Genopret', 'Modtag'])]
    cols = ['Case Number', 'Name', 'CPR', 'DQ-number', 'From Date', 'Action', 'manager email']
    filtered_signflow_df = filtered_signflow_df[cols]

    def parse_date(date_str):
        for fmt in ("%d.%m.%Y", "%d.%m.%y"):
            try:
                date_str = date_str.strip()
                return datetime.datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        return None

    today = datetime.date.today()

    filtered_signflow_df['From Date'] = filtered_signflow_df['From Date'].apply(
        lambda x: parse_date(x).strftime('%d.%m.%Y') if parse_date(x) else x
    )
    filtered_signflow_df = filtered_signflow_df[
        filtered_signflow_df['From Date'].apply(lambda x: (d := parse_date(x)) is not None and d <= today)
    ]

    try:
        for idx, row in filtered_signflow_df[filtered_signflow_df['DQ-number'].isnull()].iterrows():
            is_in_delta, dq_number = delta_client.get_dq_numbers(row['CPR'], row['From Date'])
            filtered_signflow_df.at[idx, 'DQ-number'] = dq_number
            filtered_signflow_df.at[idx, 'Findes ikke i Delta'] = None if is_in_delta else 'x'
        pd.set_option('display.max_columns', None)

        filtered_signflow_df['From Date'] = filtered_signflow_df['From Date'].apply(
            lambda x: parse_date(x).strftime('%d.%m.%Y') if parse_date(x) else x
        )
        filtered_signflow_df = filtered_signflow_df.rename(columns={
            'Case Number': 'Sagsnummer',
            'Name': 'Navn',
            'DQ-number': 'Loginnavn(e)',
            'From Date': 'Fra dato',
            'Action': 'Handling',
            'manager email': 'lederemail'
        })

        filtered_signflow_df = filtered_signflow_df.assign(
            _sort_date=filtered_signflow_df['Fra dato'].apply(lambda x: datetime.datetime.strptime(x, '%d.%m.%Y'))
        ).sort_values(by='_sort_date', ascending=True).drop(columns=['_sort_date'])

        all_institutions = sd_client.get_all_institutions_df()['InstitutionIdentifier'].values.tolist()
        if not all_institutions:
            raise ValueError("SD error")

        not_in_delta_df = filtered_signflow_df[filtered_signflow_df['Findes ikke i Delta'] == 'x']

        for idx, row in not_in_delta_df.iterrows():
            cpr = row['CPR']
            fra_dato_str = row['Fra dato']
            fra_dato = datetime.datetime.strptime(fra_dato_str, '%d.%m.%Y').date() if fra_dato_str else None
            found = False
            for institution_id in all_institutions:
                res = sd_client.person_exist(institution_id, cpr, fra_dato)
                if not isinstance(res, bool):
                    raise TypeError("SD error")
                if res:
                    found = True
                    break
            filtered_signflow_df.at[idx, 'Findes ikke i SD'] = None if found else 'x'

        filtered_signflow_df.to_excel('liste_autorisationer.xlsx', index=False)
        excel_file = df_to_excel_bytes(filtered_signflow_df)
        send_mail_with_attachment(
            IT_SUPPORT_MAIL,
            SD_DELTA_FROM_MAIL,
            f'Autorisationer for {today.strftime("%d.%m.%Y")}',
            'Liste af autorisationer er vedhæftet.',
            'liste_autorisationer.xlsx',
            excel_file
        )

        logger.info("IT Support Authorization List job completed successfully.")

        return True

    except Exception as e:
        logger.error(f"Error in IT Support Authorization List job: {e}")
        try:
            filtered_signflow_df.to_excel('liste_autorisationer.xlsx', index=False)
            excel_file = df_to_excel_bytes(filtered_signflow_df)
            send_mail_with_attachment(
                IT_SUPPORT_MAIL,
                SD_DELTA_FROM_MAIL,
                f'Autorisationer for {today.strftime("%d.%m.%Y")}',
                'Liste af autorisationer er vedhæftet. Der kan være fejl, fx. manglende DQ-numre.',
                'liste_autorisationer.xlsx',
                excel_file
            )
            logger.warning('IT Support Authorization List job partially completed.')
            return True
        except Exception as e2:
            logger.error(f"Error in IT Support Authorization List job: {e2}")
            return False
