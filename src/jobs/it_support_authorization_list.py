
import datetime
import logging
import traceback
import pandas as pd

from mail import send_mail_with_attachment
from logiva_signflow import LogivaSignflowClient
from delta_client import DeltaClient
from sd_client import SDClient
from utils.api_requests import APIClient
from utils.utils import df_to_excel_bytes
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

        all_institutions_df = sd_client.get_all_institutions_df()
        all_institutions_df = all_institutions_df[~all_institutions_df['InstitutionName'].str.contains("UDGÅET", case=False, na=False)]

        filtered_signflow_df = filtered_signflow_df.rename(columns={
            'Case Number': 'Sagsnummer',
            'Name': 'Navn',
            'DQ-number': 'Loginnavn(e)',
            'From Date': 'Fra dato Signflow',
            'Action': 'Handling',
            'manager email': 'lederemail'
        })

        filtered_signflow_df['Findes ikke i SD'] = None

        for idx, row in filtered_signflow_df[filtered_signflow_df['Handling'].isin(['Nyansat', 'Modtag'])].iterrows():
            original_date = datetime.datetime.strptime(row['Fra dato Signflow'], '%d.%m.%Y').date()

            list_of_dates = []
            for weeks in range(0, 5):
                list_of_dates.append(original_date + datetime.timedelta(weeks=weeks))

            sd_dates = []

            for date in list_of_dates:
                for inst_id in all_institutions_df['InstitutionIdentifier'].values.tolist():
                    start_date_sd = sd_client.get_employment_start_date(
                        inst_id,
                        row['CPR'],
                        date
                    )
                    if start_date_sd:
                        start_date_sd_formatted = [datetime.datetime.strptime(sd, "%Y-%m-%d").date() for sd in start_date_sd]
                        sd_dates.extend(start_date_sd_formatted)

            sd_dates = list(set([d for d in sd_dates if d is not None]))
            if sd_dates:
                sd_date = min(sd_dates, key=lambda d: abs((d - original_date).days))
                filtered_signflow_df.at[idx, 'Fra dato SD'] = sd_date.strftime("%d.%m.%Y")
            else:
                filtered_signflow_df.at[idx, 'Fra dato SD'] = None
                filtered_signflow_df.at[idx, 'Findes ikke i SD'] = 'x'

        for idx, row in filtered_signflow_df[filtered_signflow_df['Loginnavn(e)'].isnull()].iterrows():
            sf_date_is_in_delta, sf_date_dq_number = delta_client.get_dq_numbers(row['CPR'], row['Fra dato Signflow'])
            sd_date_is_in_delta, sd_date_dq_number = delta_client.get_dq_numbers(row['CPR'], row['Fra dato SD'])
            combined_dq_numbers = list(set(sf_date_dq_number + sd_date_dq_number))
            filtered_signflow_df.at[idx, 'Loginnavn(e)'] = ', '.join(combined_dq_numbers) if combined_dq_numbers else None
            filtered_signflow_df.at[idx, 'Findes ikke i Delta'] = 'x' if not (sf_date_is_in_delta or sd_date_is_in_delta) else None
        pd.set_option('display.max_columns', None)

        filtered_signflow_df = filtered_signflow_df.assign(
            _sort_date=filtered_signflow_df['Fra dato Signflow'].apply(lambda x: datetime.datetime.strptime(x, '%d.%m.%Y'))
        ).sort_values(by='_sort_date', ascending=True).drop(columns=['_sort_date'])

        all_institutions = sd_client.get_all_institutions_df()['InstitutionIdentifier'].values.tolist()
        if not all_institutions:
            raise ValueError("SD error")

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
        tb = traceback.format_exc()
        logger.error(f"Error in IT Support Authorization List job: {e}\nTraceback:\n{tb}")
        try:
            filtered_signflow_df.to_excel('liste_autorisationer.xlsx', index=False)
            excel_file = df_to_excel_bytes(filtered_signflow_df)
            send_mail_with_attachment(
                IT_SUPPORT_MAIL,
                SD_DELTA_FROM_MAIL,
                f'Autorisationer for {today.strftime("%d.%m.%Y")} - NB: Fejl',
                'Liste af autorisationer er vedhæftet. Der skete en fejl, derfor mangler det meste data.',
                'liste_autorisationer.xlsx',
                excel_file
            )
            logger.warning('IT Support Authorization List job partially completed.')
            return True
        except Exception as e2:
            logger.error(f"Error in IT Support Authorization List job: {e2}")
            return False
