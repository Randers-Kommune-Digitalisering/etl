
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

    def include_row(row):
        date_obj = parse_date(row['From Date'])
        if date_obj is None:
            return False
        if row['Action'] == 'Nyansat':
            return date_obj <= today + datetime.timedelta(days=14)
        else:
            return date_obj <= today

    filtered_signflow_df = filtered_signflow_df[
        filtered_signflow_df.apply(include_row, axis=1)
    ]

    try:
        for idx, row in filtered_signflow_df[filtered_signflow_df['DQ-number'].isnull()].iterrows():
            is_in_delta, dq_number, tjenestenummre, institution_codes = delta_client.get_dq_numbers_and_sd_lookup_data(row['CPR'], row['From Date'])
            filtered_signflow_df.at[idx, 'DQ-number'] = dq_number
            filtered_signflow_df.at[idx, 'Tjenestenummer'] = tjenestenummre
            filtered_signflow_df.at[idx, 'Institutionskode'] = institution_codes
            filtered_signflow_df.at[idx, 'Findes ikke i Delta'] = None if is_in_delta else 'x'
        pd.set_option('display.max_columns', None)

        filtered_signflow_df['From Date'] = filtered_signflow_df['From Date'].apply(
            lambda x: parse_date(x).strftime('%d.%m.%Y') if parse_date(x) else x
        )
        filtered_signflow_df = filtered_signflow_df.rename(columns={
            'Case Number': 'Sagsnummer',
            'Name': 'Navn',
            'DQ-number': 'Loginnavn(e)',
            'From Date': 'Fra dato Signflow',
            'Action': 'Handling',
            'manager email': 'lederemail'
        })

        filtered_signflow_df = filtered_signflow_df.assign(
            _sort_date=filtered_signflow_df['Fra dato Signflow'].apply(lambda x: datetime.datetime.strptime(x, '%d.%m.%Y'))
        ).sort_values(by='_sort_date', ascending=True).drop(columns=['_sort_date'])

        all_institutions = sd_client.get_all_institutions_df()['InstitutionIdentifier'].values.tolist()
        if not all_institutions:
            raise ValueError("SD error")

        not_in_delta_df = filtered_signflow_df[filtered_signflow_df['Findes ikke i Delta'] == 'x']

        for idx, row in not_in_delta_df.iterrows():
            cpr = row['CPR']
            fra_dato_str = row['Fra dato Signflow']
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

        for idx, row in filtered_signflow_df[filtered_signflow_df['Handling'].isin(['Nyansat', 'Modtag'])].iterrows():
            tjenestenummer = row.get('Tjenestenummer')
            if isinstance(tjenestenummer, str):
                tjenestenummer_list = [t.strip() for t in tjenestenummer.split(',') if t.strip()]
            else:
                tjenestenummer_list = []

            if len(tjenestenummer_list) == 1:
                for inst_id in row['Institutionskode'].split(','):
                    start_date_sd = sd_client.get_employment_start_date(
                        inst_id,
                        row['CPR'],
                        tjenestenummer_list[0],
                        datetime.datetime.strptime(row['Fra dato Signflow'], '%d.%m.%Y').date()
                    )
                    if start_date_sd is not None:
                        try:
                            start_date_sd_formatted = datetime.datetime.strptime(start_date_sd, "%Y-%m-%d").strftime("%d.%m.%Y")
                        except Exception:
                            start_date_sd_formatted = start_date_sd
                        filtered_signflow_df.at[idx, 'Fra dato SD'] = start_date_sd_formatted
                    else:
                        filtered_signflow_df.at[idx, 'Fra dato SD'] = start_date_sd

        for col in ['Tjenestenummer', 'Institutionskode']:
            if col in filtered_signflow_df.columns:
                filtered_signflow_df = filtered_signflow_df.drop(columns=[col])

        cols = list(filtered_signflow_df.columns)
        if 'Fra dato SD' in cols and 'Fra dato Signflow' in cols:
            cols.remove('Fra dato SD')
            idx = cols.index('Fra dato Signflow') + 1
            cols.insert(idx, 'Fra dato SD')
            filtered_signflow_df = filtered_signflow_df[cols]

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
