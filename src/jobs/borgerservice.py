import pymssql
import pandas as pd
import logging
import io
import numpy as np

from prophet import Prophet
from datetime import datetime

from utils.config import FRONTDESK_DB_USER, FRONTDESK_DB_PASS, FRONTDESK_DB_HOST, FRONTDESK_DB_DATABASE
from custom_data_connector import post_data_to_custom_data_connector

logging.getLogger("prophet.plot").disabled = True
logger = logging.getLogger(__name__)


def job():
    logger.info("Initializing frontdesk borgerservice job")

    try:
        workdatasets = connectToFrontdeskDB()
    except Exception as e:
        logger.error(e)
        logger.error("Failed to connect to Frontdesk Borgerservice database")
        return False
    else:
        logger.info("Connected to Frontdesk Borgerservice database successfully")
        workdata = transformations(workdatasets)
        workdata_grouped = dailyVisitors(workdata)
        logger.info("Data transformed successfully")

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

        # Upload forcasts
        file = io.BytesIO(predictions.to_csv(index=False, sep=';').encode('utf-8'))
        filename = "SA" + "FrontdeskBorgerserviceForecasts" + ".csv"

        if post_data_to_custom_data_connector(filename, file):
            logger.info("Updated Frontdesk Borgerservice forecasts successfully")
        else:
            logger.error("Failed to update Frontdesk Borgerservice forecasts")
            return False
       
        # save_data(predictions,'FrontdeskBorgerserviceForecasts')
        workdata = transformationsBeforeUpload(workdata)
        
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

    table_names = ["Operation", "Registration", "Ticket"]
    tables = {}
    for table in table_names:
        cursor.execute(f"SELECT * FROM {table}")
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        tables[table] = pd.DataFrame(rows, columns=columns)
    
    return tables

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


def transformations(input_data):
    # Opslitter på enkelte dataframes
    operation = input_data['Operation']
    ticket = input_data['Ticket'] 
    registration = input_data['Registration']
    logger.info("1")

    # Dropper rækker
    # Dropper betjeninger der ikke er afsluttet
    operation = operation[operation['State'] == "Ended"]
    logger.info("2")

    # Dropper obervationer der ikke fra borgerservice
    operation.drop(operation[operation.CounterName.isin(['Jobcenter', 'Ydelseskontoret', 'Integration'])].index, inplace=True)
    logger.info("3")

    # Dropper oberservation ældre end to år eller før 1/1/2023
    dateTwoYearsBefore = datetime(datetime.now().year - 2, datetime.now().month, datetime.now().day)
    operation.drop(operation[(operation.CreatedAt < datetime(2023, 1, 1)) | (operation.CreatedAt < dateTwoYearsBefore)].index, inplace=True)

    operation['dato'] = operation['CreatedAt'].dt.date
    operation['ugenr'] = operation['CreatedAt'].dt.isocalendar().week
    operation['år'] = operation['CreatedAt'].dt.year
    logger.info("4")

    # Beriger operation-data
    # Tilføjer tickettype til operation
    ticket = ticket[['Id','Tickettype']]

    operation=pd.merge(operation, ticket, left_on='TicketId', right_on='Id', how='left', indicator=True)
    operation.drop(columns=['_merge'], inplace=True)
    operation.drop(columns=['Id_y'], inplace=True)
    operation.rename(columns={'Id_x':'Id'}, inplace=True)
    logger.info("5")

    # Identificerer og tilføjer underbetjeninger fra registration
    registration = registration[['OperationId','QueueName','QueueCategoryName']]

    operation=pd.merge(operation, registration, left_on='Id', right_on='OperationId', how='left', indicator=True)
    operation.drop(columns=['_merge'], inplace=True)

    operation['registreringNr']=operation.groupby('Id').cumcount()+1  # row_number for each Id
    operation['antalRegistreringer']=operation.groupby('Id')['registreringNr'].transform('max') # max row_number for each Id
    operation['Tickettype'] = np.where(operation['registreringNr'] > 1, 'Underbetjening', operation['Tickettype']) # if row_number > 1 then Underbetjening
    logger.info("6")

    # Drop columns
    operation = operation.drop(columns=['MunicipalityID', 'QueueId', 'QueueCategoryId', 'StateId', 'CounterId', 'EmployeeId', 'DelayedUntil', 'DelayedFrom', 'IsEmployeeAnonymized', 'EmployeeInitials'])
    
    # Transform data types
    operation['CreatedAt'] = pd.to_datetime(operation['CreatedAt']).dt.tz_localize(None)
    operation['CalledAt'] = pd.to_datetime(operation['CalledAt']).dt.tz_localize(None)
    operation['EndedAt'] = pd.to_datetime(operation['EndedAt']).dt.tz_localize(None)
    operation['LastAggregatedDataUpdateTime'] = pd.to_datetime(operation['LastAggregatedDataUpdateTime']).dt.tz_localize(None)
    logger.info("7")

    # Gruppering af køer
    operation['QueuesGrouped'] = operation.apply(groupQueues, axis=1)
    logger.info("8")

    # Justering af tidsvariable
    operation['BehandlingstidMinutter'] = operation['EndedAt'] - operation['CalledAt']
    operation['BehandlingstidMinutterDecimal'] = (operation['AggregatedProcessingTime'] / (10**7 * 60)).round(2)
    operation['VentetidMinutter'] = operation['CalledAt'] - operation['CreatedAt']
    logger.info("9")

    # AggregatedWaitingTime giver ikke rigtig mening
    # operation['VentetidMinutterDecimal'] = (operation['AggregatedWaitingTime'] / (10**7 * 60)).round(2)
    operation['VentetidMinutterDecimal'] = (operation['VentetidMinutter'].dt.total_seconds() / 60).round(2)
    logger.info("10")

    return operation

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
    model = Prophet(changepoints=['2023-11-23'], yearly_seasonality=True, weekly_seasonality=True, seasonality_mode='multiplicative', holidays=holidays)

    model.fit(data_model)

    future = model.make_future_dataframe(periods=365)
    future['floor'] = 0
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
