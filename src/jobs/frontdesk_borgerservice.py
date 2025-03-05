import pymssql
import pandas as pd
import logging
import io
from prophet import Prophet
from datetime import datetime
from utils.config import FRONTDESK_DB_USER, FRONTDESK_DB_PASS, FRONTDESK_DB_HOST, FRONTDESK_DB_DATABASE
from utils.database_connection import get_db_frontdesk

from custom_data_connector import post_data_to_custom_data_connector


logger = logging.getLogger(__name__)

db_client = get_db_frontdesk()


def job():
    logger.info("Initializing frontdesk borgerservice job")

    try:
        workdata = connectToFrontdeskDB()
        # workdata = pd.read_csv('data/FrontdeskBorgerserviceTables.csv', sep=';')
    except Exception as e:
        logger.error(e)
        logger.error("Failed to connect to Frontdesk Borgerservice database")
        return False
    else:
        logger.info("Connected to Frontdesk Borgerservice database successfully")
        workdata = transformations(workdata)
        workdata_grouped = dailyVisitors(workdata)

        # Forecast på alle køer
        try:
            predictions = prophet(workdata_grouped, 'samlet')
        except Exception as e:
            logger.error(e)
            logger.error("Failed to forecast all queues")
        else:
            logger.info("Forecasted all queues successfully")

        queues = ['Pas', 'MitID', 'Afhent pas/kørekort/sundhedskort',
                  'Kørekort', 'Pension', 'Informationen', 'Buskort til pensionister',
                  'Andet', 'Beboerindskud og boligstøtte', 'Skat', 'Flytning og Folkeregister',
                  'Sundhedskort og lægevalg', 'Legitimationskort', 'Sundhedskort og lægevalg',
                  'Tilflytning fra udlandet', 'Fritagelse for digitalpost']

        for queue in queues:
            workdata_temp = workdata.drop(workdata[workdata.QueuesGrouped != queue].index)
            workdata_grouped = dailyVisitors(workdata_temp)
            try:
                predictions_grouped = prophet(workdata_grouped, queue)
            except Exception as e:
                logger.error(e)
                logger.error(f"Failed to forecast {queue}")
                continue
            else:
                logger.info(f"Forecasted {queue} successfully")
                predictions = pd.concat([predictions, predictions_grouped], axis=0)

        # logger.info(predictions.info())
        # logger.info(predictions.describe())
        # Create a connection to the PostgreSQL server

        # Upload forecasts to PostgreSQL
        try:
            db_client.ensure_database_exists()
            connection = db_client.get_connection()
            if connection:
                logger.info("Attempting to upload forecasts to PostgreSQL")
                predictions.to_sql('forecasts', con=connection, if_exists='replace', index=False)
                logger.info("Updated Frontdesk Borgerservice forecasts successfully in PostgreSQL")
                connection.close()
            else:
                logger.error("Failed to connect to PostgreSQL")
                return False
        except Exception as e:
            logger.error(e)
            logger.error("Failed to update Frontdesk Borgerservice forecasts in PostgreSQL")
            return False

        # Upload operations to PostgreSQL
        try:
            db_client.ensure_database_exists()
            if connection:
                logger.info("Attempting to upload forecasts to PostgreSQL")
                predictions.to_sql('operations', con=connection, if_exists='replace', index=False)
                logger.info("Updated Frontdesk Borgerservice operations successfully in PostgreSQL")
                connection.close()
            else:
                logger.error("Failed to connect to PostgreSQL")
                return False
        except Exception as e:
            logger.error(e)
            logger.error("Failed to update Frontdesk Borgerservice operations in PostgreSQL")
            return False

        # Upload forcasts
        # predictions.to_csv('data/Forecasts.csv', index=False, sep=';')
        file = io.BytesIO(predictions.to_csv(index=False, sep=';').encode('utf-8'))
        filename = "SA" + "FrontdeskBorgerserviceForecasts" + ".csv"

        if post_data_to_custom_data_connector(filename, file):
            logger.info("Updated Frontdesk Borgerservice forecasts successfully")
        else:
            logger.error("Failed to update Frontdesk Borgerservice forecasts")
            return False

        # save_data(predictions,'FrontdeskBorgerserviceForecasts')
        workdata = transformationsBeforeUpload(workdata)
        # workdata.to_csv('data/Operation.csv', index=False, sep=';')

        file = io.BytesIO(workdata.to_csv(index=False, sep=';').encode('utf-8'))
        filename = "SA" + "FrontdeskBorgerservice" + ".csv"

        if post_data_to_custom_data_connector(filename, file):
            logger.info("Updated Frontdesk Borgerservice data successfully")
            return True
        else:
            logger.error("Failed to update Frontdesk Borgerservice data")
            return False


def connectToFrontdeskDB():
    conn = pymssql.connect(FRONTDESK_DB_HOST, FRONTDESK_DB_USER, FRONTDESK_DB_PASS, FRONTDESK_DB_DATABASE)
    cursor = conn.cursor()

    tables = ["Operation"]
    for table in tables:
        cursor.execute(f"SELECT * FROM {table}")
        rows = cursor.fetchall()
        # logger.info(cursor.description)
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)

    return df


def groupQueues(row):
    if row['QueueName'] in ['Afhent pas/kørekort/sundhedskort ']:
        return 'Afhent pas/kørekort/sundhedskort'
    elif row['QueueName'] in ['Beboerindskud', 'Beboerindskud og boligstøtte ', 'Boligstøtte']:
        return 'Beboerindskud og boligstøtte'
    elif row['QueueName'] in ['Den Boligsociale Enhed - Anders', 'Den Boligsociale Enhed  - Janni']:
        return 'Den Boligsociale Enhed'
    elif row['QueueName'] in ['Flytning og folkeregister ', 'Flytning og Folkeregister']:
        return 'Flytning og Folkeregister'
    elif row['QueueName'] in ['Kontrolenheden - Bjørn', 'Kontrolenheden - Erik', 'Kontrolenheden - Marianne']:
        return 'Kontrolenheden'
    elif row['QueueName'] in ['Kørekort ', 'Kørekort Selfie']:
        return 'Kørekort'
    elif row['QueueName'] in ['Legitimationskort/ID-kort', 'Legitimationskort ']:
        return 'Legitimationskort'
    elif row['QueueName'] in ['MitID ', 'MitID', 'Mit-ID Aktiveringskode']:
        return 'MitID'
    elif row['QueueName'] in ['Pas ', 'Pas Selfie']:
        return 'Pas'
    elif row['QueueName'] in ['Pension ', 'Pension', 'Pension akut tid']:
        return 'Pension'
    elif row['QueueName'] in ['Resultat af årsopgørelse', 'SKAT', 'Skat']:
        return 'Skat'
    else:
        return row['QueueName']


def transformations(data):
    # Drop columns
    data = data.drop(columns=['MunicipalityID', 'QueueId', 'QueueCategoryId', 'State', 'StateId', 'CounterId', 'EmployeeId', 'DelayedUntil', 'DelayedFrom', 'IsEmployeeAnonymized', 'EmployeeInitials'])

    # Transform data types
    data['CreatedAt'] = pd.to_datetime(data['CreatedAt']).dt.tz_localize(None)
    data['CalledAt'] = pd.to_datetime(data['CalledAt']).dt.tz_localize(None)
    data['EndedAt'] = pd.to_datetime(data['EndedAt']).dt.tz_localize(None)
    data['LastAggregatedDataUpdateTime'] = pd.to_datetime(data['LastAggregatedDataUpdateTime']).dt.tz_localize(None)

    # Dropper rækker
    # Data ikke fra borgerservice
    data.drop(data[data.CounterName.isin(['Jobcenter', 'Ydelseskontoret', 'Integration'])].index, inplace=True)

    # data ældre end to år eller før 1/1/2023
    dateTwoYearsBefore = datetime(datetime.now().year - 2, datetime.now().month, datetime.now().day)
    data.drop(data[(data.CreatedAt < datetime(2023, 1, 1)) | (data.CreatedAt < dateTwoYearsBefore)].index, inplace=True)

    data['dato'] = data['CreatedAt'].dt.date
    data['ugenr'] = data['CreatedAt'].dt.isocalendar().week
    data['år'] = data['CreatedAt'].dt.year

    # Gruppering af køer
    data['QueuesGrouped'] = data.apply(groupQueues, axis=1)

    # Justering af tidsvariable
    data['BehandlingstidMinutter'] = data['EndedAt'] - data['CalledAt']
    data['BehandlingstidMinutterDecimal'] = (data['AggregatedProcessingTime'] / (10**7 * 60)).round(2)
    data['VentetidMinutter'] = data['CalledAt'] - data['CreatedAt']

    # AggregatedWaitingTime giver ikke rigtig mening
    # data['VentetidMinutterDecimal'] = (data['AggregatedWaitingTime'] / (10**7 * 60)).round(2)
    data['VentetidMinutterDecimal'] = (data['VentetidMinutter'].dt.total_seconds() / 60).round(2)

    return data


def transformationsBeforeUpload(data):
    # Til evt. senere brug
    return data


def dailyVisitors(data):
    data = data.groupby(['dato']).size().reset_index()
    data.columns = ['dato', 'antal']

    return data


def prophet(data, forecastModel):

    # Prophet
    holidays = pd.DataFrame({
        'holiday': 'lukkedage',
        'ds': pd.to_datetime([
            '2023-01-01', '2024-01-01', '2025-01-01', '2026-01-01',  # Nytårsdag
            '2023-04-13', '2024-03-29', '2025-04-18', '2026-04-03',  # Skærtorsdag
            '2023-04-14', '2024-03-30', '2025-04-19', '2026-04-04',  # Langfredag
            '2023-04-17', '2024-04-01', '2025-04-21', '2026-04-06',  # 2. Påskedag
            '2023-05-05',  # Store Bededag
            '2023-05-25', '2024-05-16', '2025-06-05', '2026-05-21',  # Kr. Himmelfartsdag
            '2023-06-04', '2024-05-26', '2025-06-15', '2026-05-31',  # 2. Pinsedag
            '2023-12-25', '2024-12-25', '2025-12-25', '2026-12-25',  # 1. Juledag
            '2023-12-26', '2024-12-26', '2025-12-26', '2026-12-26',  # 2. Juledag
            '2023-12-31', '2024-12-31', '2025-12-31', '2026-12-31',  # Nytårsaften
        ]),
        'lower_window': 0,
        'upper_window': 1,
    })

    data_model = data.rename(columns={'dato': 'ds', 'antal': 'y'})
    model = Prophet(changepoints=['2023-11-23'], yearly_seasonality=True, weekly_seasonality=True, seasonality_mode='multiplicative', holidays=holidays, growth='logistic')
    data_model['cap'] = 500
    data_model['floor'] = 1
    model.fit(data_model)

    future = model.make_future_dataframe(periods=365)
    future['cap'] = 500
    future['floor'] = 1
    future = future[future['ds'].dt.weekday.isin([0, 1, 2, 3, 4])]
    forecast = model.predict(future)

    # data = pd.merge(data,forecast[['ds','yhat','yhat_lower','yhat_upper']], left_on='dato', right_on='ds', how='left')
    data = pd.concat([forecast[['ds', 'yhat']], data['antal']], axis=1)
    data.rename(columns={'ds': 'dato'}, inplace=True)

    # data['yhat'] = data['yhat'].mask(data['antal'].notna())
    # data['antal'].fillna(data['yhat'], inplace=True)
    data['model'] = forecastModel
    data = data[['dato', 'model', 'antal', 'yhat']]
    data['antal'] = data['antal'].round(2)
    data['yhat'] = data['yhat'].round(2)

    # fig1 = model.plot(forecast)
    # fig2 = model.plot_components(forecast)

    return data
