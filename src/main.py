from flask import Flask
from healthcheck import HealthCheck
from prometheus_client import generate_latest
import logging
from utils.database_connection import get_asset_db
from capa.capa_data import create_capa_table_if_not_exists

from utils.logging import set_logging_configuration, APP_RUNNING
from utils.config import DEBUG, PORT, POD_NAME
from job_endpoints import job_api_bp
import pandas as pd

logger = logging.getLogger(__name__)


def create_app():
    app = Flask('ETL')
    health = HealthCheck()
    app.add_url_rule("/healthz", "healthcheck", view_func=lambda: health.run())
    app.add_url_rule('/metrics', "metrics", view_func=generate_latest)
    app.register_blueprint(job_api_bp)
    APP_RUNNING.labels(POD_NAME).set(1)
    return app


def initialize_db():
    asset_db_client = get_asset_db()
    create_capa_table_if_not_exists(asset_db_client)
    logger.info("Initialization complete.")


set_logging_configuration()
app = create_app()

if __name__ == "__main__":  # pragma: no cover
    # initialize_db()
    # app.run(debug=DEBUG, host='0.0.0.0', port=PORT)
    from sd_delta import delta_client
    import pandas as pd
    # import os

    excel_file = "afdelinger.xlsx"
    orgs = delta_client.get_adm_orgs()

    # Collect all dataframes and sheet names first
    dfs = []
    sheet_names = []

    for org in orgs:
        if org['name'].lower().strip() not in ['politikerne', 'borgmesterkontoret', 'handicaprådet', 'eksterne']:
            employees = delta_client.get_employees(org['userkey'])
            if len(employees) > 1:
                leaders = delta_client.get_leaders(org['userkey'])
                for leader in leaders:
                    employees.append(leader)
                combined = [dict(t) for t in {tuple(emp.items()) for emp in employees}]
                df = pd.DataFrame(combined)
                df['unit'] = org['name']
                df = df.sort_values(by='user', ascending=False, na_position='last')
                dfs.append(df)
                # Sheet names must be <=31 chars and not contain certain characters
                safe_sheet_name = org['name'][:31].replace('/', '_').replace('\\', '_').replace('*', '_').replace('?', '_').replace('[', '_').replace(']', '_').replace(':', '_')
                sheet_names.append(safe_sheet_name)

    combined_df = pd.concat(dfs, ignore_index=True)

    with pd.ExcelWriter(excel_file, engine='openpyxl', mode='w') as writer:
        combined_df.to_excel(writer, sheet_name="Alle", index=False)
        for df, sheet_name in zip(dfs, sheet_names):
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    # input_file = "grupper.xlsx"
    # output_file = "afdelinger.xlsx"

    # # Read all sheets
    # all_sheets = pd.read_excel(input_file, sheet_name=None)

    # # Add 'unit' column to each sheet
    # for sheet_name, df in all_sheets.items():
    #     df['unit'] = sheet_name
    #     all_sheets[sheet_name] = df

    # # Combine all sheets into one DataFrame
    # combined_df = pd.concat(all_sheets.values(), ignore_index=True)

    # # Write to new Excel file with "Alle" as the first sheet
    # with pd.ExcelWriter(output_file, engine='openpyxl', mode='w') as writer:
    #     combined_df.to_excel(writer, sheet_name="Alle", index=False)
    #     for sheet_name, df in all_sheets.items():
    #         df.to_excel(writer, sheet_name=sheet_name, index=False)
    
