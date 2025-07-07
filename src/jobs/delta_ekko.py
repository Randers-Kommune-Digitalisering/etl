from sd_delta import delta_client, sd_client
from datetime import datetime
import pandas as pd


def job():
    inst_id = 'RG'
    sd_codes_to_include = [
        "TBVH",
        "TESS",
        "TBDE",
        "TRÅ1",
        "TKAN",
        "TRYK",
        "TBEN",
        "TRD1",
        "TRD2",
        "TRD3",
        "TRD4",
        "TTSA",
        "TTS1",
        "TTS2",
        "TTS3",
        "TTS4"
    ]

    # extra_sd_codes = [
    #     "TDRI",
    #     "TTSV",
    #     "TKIR",
    #     "TRKK",
    #     "TMAT"
    # ]

    # adm_orgs = delta_client.get_adm_children(top_adm_org_user_key="99921566")
    all_ekko_employees = []

    # for adm_org in adm_orgs:
    for sd_department_id in sd_codes_to_include:
        # employees = delta_client.get_employees_by_adm_org(adm_org_name=adm_org['name'])
        employees = delta_client.get_employees_by_sd_department(sd_department_id=sd_department_id)
        dep_start_dates = sd_client.get_all_start_dates(institution_id=inst_id, department_id=sd_department_id)
        start_dates = {}
        for sd in dep_start_dates:
            start_dates[sd['employment_id']] = datetime.strptime(sd['employment_date'], "%Y-%m-%d").strftime("%d-%m-%Y")

        for emp in employees:
            emp['Ansættelsesdato'] = start_dates.get(emp['Personalenr.'], None)
            all_ekko_employees.append(emp)

    #     for tup in unique_ins_dep_tuples:
    #         if True:
    #         # if tup[1] in sd_codes_to_include:
    #             dep_start_dates = sd_client.get_all_start_dates(institution_id=tup[0], department_id=tup[1])
    #            

    #     for emp in employees:
    #         if True:
    #         # if emp['UserGroup'] not in sd_codes_to_include:
    #             # start_dates = sd_client.get_employment_start_date(institution_id=emp['institution_code'], employment_id=emp['Personalenr.'])
    #             emp['Ansættelsesdato'] = start_dates.get(emp['Personalenr.'], None)
    #             del emp['institution_code']
    #             all_ekko_employees.append(emp)

    df = pd.DataFrame(all_ekko_employees)
    df.to_csv(f"ekko-brugere-{datetime.now().strftime('%d-%m-%Y')}.csv", index=False, sep=';', encoding='cp1252')

    return True
