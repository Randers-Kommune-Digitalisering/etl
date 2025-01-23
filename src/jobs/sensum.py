import logging
import urllib.parse
from sensum.sensum import process_sensum, handle_files, merge_dataframes, process_sensum_latest, files_to_df
from sensum.sensum_data import sensum_data_merge_df
from sensum.sensum_sags_aktiviteter import sags_aktiviteter_merge_df
from utils.api_requests import APIClient
from utils.config import CONFIG_LIBRARY_USER, CONFIG_LIBRARY_PASS, CONFIG_LIBRARY_URL, CONFIG_LIBRARY_BASE_PATH, SENSUM_CONFIG_FILE
from sensum.sensum_ydelse import merge_df_ydelse
from sensum.sensum_administrative_forhold import merge_df_administrative_forhold
from sensum.sensum_mål import merge_df_sensum_mål

logger = logging.getLogger(__name__)

config_library_client = APIClient(base_url=CONFIG_LIBRARY_URL, username=CONFIG_LIBRARY_USER, password=CONFIG_LIBRARY_PASS)

# TODO: if process_func is always the same, then don't pass it as an argument - if not always the same, set it with the config file + switch case, if else or dynamicly import the function
# TODO: Same for merge_func
# TODO: Fix and then remove comments + remove unused code


def job():
    try:
        logger.info('Starting Sensum ETL job!')

        config_path = urllib.parse.urljoin(CONFIG_LIBRARY_BASE_PATH, SENSUM_CONFIG_FILE)
        sensum_jobs_config = config_library_client.make_request(path=config_path)
        if sensum_jobs_config is None:
            logging.error(f"Failed to load config file from path: {config_path}")
            return False

        results = []
        for config in sensum_jobs_config:
            results.append(process_sensum(
                config['patterns'],
                handle_files,  # Could be in the config file, why else are you parsing this to "process_sensum"?
                lambda *dfs: merge_dataframes(
                    *dfs, config['merge_on'], config['group_by'], config['agg_columns'], config['columns']
                ),
                config['name']
            ))
        results.append(process_sensum(
            ['Sager_*.csv', 'SagsAktiviteter_*.csv', 'Borger_*.csv'],  # add to the config file?
            handle_files,  # Could be in the config file, why else are you parsing this to "process_sensum" in the config loop above?
            sags_aktiviteter_merge_df,  # Could be in the config file, why else are you parsing this to "process_sensum" in the config loop above?
            "SASensumSagsAktivitet.csv"  # add to the config file? and shouldn't it be "SA_SensumSagsAktivitet.csv"?!
        ))
        results.append(process_sensum(
            ['Sager_*.csv', 'Indsatser_*.csv', 'Borger_*.csv'],  # add to the config file?
            handle_files,  # Could be in the config file, why else are you parsing this to "process_sensum" in the config loop above?
            sensum_data_merge_df,  # Could be in the config file, why else are you parsing this to "process_sensum" in the config loop above?
            "SASensum.csv"  # add to the config file? and shouldn't it be "SA_Sensum.csv"?!
        ))
        results.append(process_sensum_latest(
            ['Ydelse_*.csv', 'Borger_information_*.csv', 'Afdeling_*.csv'],  # add to the config file?
            ['Baa', 'BeVej', 'BoAu', 'Born_Bo', 'Bvh', 'Frem', 'Hjorne', 'Job', 'Lade', 'Lbg', 'Meau', 'Mepu', 'P4', 'Phus', 'Psyk', 'Senhj', 'STU'],  # add to the config file
            files_to_df,   # Could be in the config file, why else are you parsing this to "process_sensum" in the config loop above?
            merge_df_ydelse,  # Could be in the config file, why else are you parsing this to "process_sensum" in the config loop above?
            "SASensumUdførerYdelse.csv"  # add to the config file? and shouldn't it be "SA_SensumUdførerYdelse.csv"?!
        ))
        results.append(process_sensum_latest(
            ['Borger_information_*.csv', 'Administrative_*.csv'],  # add to the config file?
            ['Baa', 'BeVej', 'BoAu', 'Born_Bo', 'Bvh', 'Frem', 'Hjorne', 'Job', 'Lade', 'Lbg', 'Meau', 'Mepu', 'P4', 'Phus', 'Psyk', 'Senhj', 'STU'],  # add to the config file
            files_to_df,   # Could be in the config file, why else are you parsing this to "process_sensum" in the config loop above?
            merge_df_administrative_forhold,  # Could be in the config file, why else are you parsing this to "process_sensum" in the config loop above?
            "SASensumUdførerAdminstrativeForhold.csv"  # add to the config file? and shouldn't it be "SA_SensumUdførerAdminstrativeForhold.csv"?!
        ))
        results.append(process_sensum_latest(
            ['Mål_*.csv', 'Delmål_*.csv', 'Borger_information_*.csv'],  # add to the config file?
            ['Baa', 'BeVej', 'BoAu', 'Born_Bo', 'Bvh', 'Frem', 'Hjorne', 'Job', 'Lade', 'Lbg', 'Meau', 'Mepu', 'P4', 'Phus', 'Psyk', 'Senhj', 'STU'],  # add to the config file
            files_to_df,   # Could be in the config file, why else are you parsing this to "process_sensum" in the config loop above?
            merge_df_sensum_mål,  # Could be in the config file, why else are you parsing this to "process_sensum" in the config loop above?
            "SASensumUdførerMål.csv"  # add to the config file? and shouldn't it be "SA_SensumUdførerMål.csv"?!
        ))
        # results.append(get_sensum_sags_aktiviteter())
        # results.append(get_sensum_data())
        # results.append(get_sensum_ydelse())
        # results.append(get_sensum_adminstrative_forhold())
        # results.append(get_sensum_mål())
        return all(results)
    except Exception as e:
        logger.error(f'An error occurred: {e}')
        return False
