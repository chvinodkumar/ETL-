
##### Sample Config #######
"""
{
    "source": "ServiceNow",
    "environment" : "dev/prod",
    "log_file_path" : "Absolute Path of the log file",
    "add_on_email_stake_holders" : "Only to be added when extra email stake holders had to be included or else neglect this key value",
    "utils_path" : "Path to the utility py file which has logging and alert feature on failure compulsory input in config",
    "src_cnct_path" : "Path of the source connection details are stored",
    "src_environment" : "Source connection profile name",
    "authenticator_type" : "Specify type of authenticator (oauth2/basic_auth)"
    "url" : "API link",
    "fields_to_fetch" : "Fields/column names to fetch from API",
    "api_limit" :  "limit of the api per call to be passed as integer (int) if pagination is required else can be left empty ''",
    "batch_size" : Chunk size to load data into target in chunks to be passed as integer (int),
    "redshift_config" : "Path to redshift configuration",
    "redshift_profile":"redshift connection profile",
    "main_table" : "Target table name",
    "stage_table" : "Stage Table name,
    "primary_key" : "Primary key column name",
    "schema_name" : "schema name",
    "cdc_column" : "cdc (capture data change) column name ",
    "load_type" : "incremental/truncate_and_load",
    "Timestamp_column" : "Ingestion audit column name"
}
"""

import sys
import os
import argparse
import json
import pandas as pd
from datetime import datetime
import traceback


try :
    ## passing the json path
    parser = argparse.ArgumentParser()
    parser.add_argument('--infile', nargs=1, help="JSON file to be processed", type=argparse.FileType('r'))
    arguments = parser.parse_args()
    config = json.load(arguments.infile[0])

    #Importing custom modules
    sys.path.insert(0,config["utils_path"])
    from utils import setup_logger, send_email_notification
    from servicenow_datafetcher import ServiceNow_DataFetcher
    from redshift_loader import Database
    from redshift_connector import get_connection

    # Create logger object
    log_filename = str(arguments.infile[0].name).split('/')[-1].replace('json', 'log')
    log_file=os.path.join(config["log_file_path"], log_filename)
    logger = setup_logger(log_file)

    #create ServiceNow_DataFetcher class object
    data_fetcher = ServiceNow_DataFetcher(logger = logger)

    #Type of Load and assign the params
    if config['load_type'] == 'truncate_and_load' :
        engine = get_connection(filepath=config["redshift_config"], profile=config["redshift_profile"], logger=logger)
        engine.execute(f'truncate table {config["schema_name"]}.{config["main_table"]};')
        logger.info(f'Truncated the main table : {config["schema_name"]}.{config["main_table"]}')
        #Configure the Params for full load
        params = {'sysparm_fields' : f"{config['fields_to_fetch']}", 'sysparm_exclude_reference_link': 'true'}
    elif config['load_type'] == 'incremental' :
        #Get max sys_updated_on timestamp from mirror
        max_tmsp = data_fetcher.get_max_date_from_redshift(config_path=config["redshift_config"], profile=config["redshift_profile"], schema=config["schema_name"], table=config["main_table"], orderby_col=config["cdc_column"])
        #Configure the Params for incremental
        params = {'sysparm_query': f'{config["cdc_column"]} > {max_tmsp}','sysparm_fields' : f"{config['fields_to_fetch']}", 'sysparm_exclude_reference_link': 'true'}

    # Load data to target in chunks
    initial_truncate_table = True
    data_loaded = False
    offset = 0

    while True:
        # Fetching DF and offset from ServiceNow_DataFetcher Module
        df, offset = data_fetcher.fetch_api_data(url=config['url'], src_cnct_path=config['src_cnct_path'], src_environment=config['src_environment'], authenticator_type=config['authenticator_type'], limit=config['api_limit'], batch_size=config['batch_size'], offset=offset, params=params)

        if df.empty :
            break

        #Check if all required all present in the DataFrame
        all_columns_present, missing_columns, extra_columns = data_fetcher.check_columns(df, config['expected_columns'])
        message = 'Expected columns fetched from the API' if all_columns_present else \
                (f'Missing columns found in the API response: {missing_columns}' if missing_columns else \
                f'Extra columns found in the API response: {extra_columns}' if extra_columns else '')
        logger.info(f"{message}")

        #Break loop if any missing columns returned in response
        if extra_columns and missing_columns:
            break

        #Adding audit column to df
        df[config['Timestamp_column']] = datetime.now()

        # Determine the load type based on the initial_truncate_table flag
        load_type = 'truncate_and_load' if initial_truncate_table else 'fullload'

        # Load data into the stage table
        Database(load_type=load_type, logger=logger, config=config["redshift_config"], profile=config["redshift_profile"], data=df, schema=config["schema_name"], main_table_name=config["stage_table"])
        initial_truncate_table=False
        data_loaded = True

    #Performing remove_duplicates_and_load after all data is loaded into the stage table
    if data_loaded :
        Database(load_type='remove_duplicates_and_load', logger=logger, config=config["redshift_config"], profile=config["redshift_profile"], data=pd.DataFrame(), schema=config["schema_name"], main_table_name=config["main_table"], stage_table_name=config["stage_table"], primary_key=config["primary_key"],orderby_col=config["cdc_column"])
        ### Success Mail Alert
        send_email_notification(message=f"Ingestion of data has been successfully completed.\n\nSource: {config['source']}\nSchema Name: {config['schema_name']}\nMain Table: {config['main_table']}\nEnvironment: {config['environment']}", subject=f"INFO - SUCCESS | {config['source']} | {config['environment']} | {config['schema_name']}.{config['main_table']}",log_path=log_file,logger=logger,add_on_email_stake_holders=config["add_on_email_stake_holders"])

except Exception as e:
    logger.error(f"Exception occured-> {e}")
    logger.error(f'Failed to Load data from Servicenow API to Redshift : Error : {e}')
    log_file_size= os.path.getsize(log_file)
    file_size=log_file_size / (1024 * 1024)
    if file_size < 1:
        ### Failure Mail Alert
        send_email_notification(message=f"Script Path-> {os.path.abspath(__file__)} \n Config Path-> {arguments.infile[0].name} \n Exception -> {e} occured \n {traceback.format_exc()}", subject=f"FATAL | {config['environment']} | {config.get('source','api')} Ingestion | {config['schema_name']}.{config['main_table']}",log_path=log_file,logger=logger,add_on_email_stake_holders=config["add_on_email_stake_holders"])
    else:
        send_email_notification(message=f"Script Path-> {os.path.abspath(__file__)} \n Config Path-> {arguments.infile[0].name} \n Exception -> {e} occured \n Log File couldn't be attached with this mail due to file size limit being exceeded, Log Path-> {log_path} \n {traceback.format_exc()}", subject=f"FATAL | {config['environment']} | {config.get('source','api')} Ingestion | {config['schema_name']}.{config['main_table']}",log_path=log_file,logger=logger,add_on_email_stake_holders=config["add_on_email_stake_holders"])
