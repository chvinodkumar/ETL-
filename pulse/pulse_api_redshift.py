
#### Sample Config ####
"""{
    "auth_url": "authentication url to generate token",
    "additional_post_parameters": "additional parameters required to hit auth_url",
    "base_url": "api url using which response can be generated",
    "log_file_path": "log file path",
    "redshift_config":"redshift config path",
    "redshift_profile": "redshift profile",
    "schema_name": "schema name",
    "main_table": "table name",
    "load_type": "truncate_and_load / incremental / fullload",
    "utils_path": "utils path",
    "environment": "Environment name",
    "source": "source name"
}"""

#### Importing Necessary Packages ####
import argparse
from datetime import datetime
import sys,os
import json
import pandas as pd
import traceback
from datetime import datetime

def response_to_dataframe(response_data : list):
    """
    A method to Convert flatten response into dataframe and insert to redshift table

    Parameters:
    response_data (List) : Name of the table which data has to be inserted

    Returns : None
    """
    try:
        logger.info("Executing response_to_dataframe method")
        df=pd.DataFrame(response_data)
        df["ingestion_timestamp"]=datetime.today()
        df.replace("", pd.NA, inplace=True) # Replace empty strings "" with pd.NA to handle null values while redshift insert
        df = df.applymap(lambda x: str(x) if isinstance(x, list) else x) # converts data from source coming as list to str (it will remain as list inside a string)
        from redshift_loader import Database # redshift_loader module imported from utils path to insert the dataframe to redshift table
        Database(load_type=config["load_type"],logger=logger,config=config["redshift_config"],profile=config["redshift_profile"],data=df,schema=config["schema_name"],main_table_name=config["main_table"],stage_table_name=config.get("stage_table",None),primary_key=config.get("primary_key",None))
    except Exception as e:
        logger.error(f"Failed to execute response_to_dataframe method with error --> {e} {traceback.format_exc()}")
        raise

def auth():
    """
    A method to hit api with provided auth and base url using api_connector module

    Parameters: None

    Returns : None
    """
    try:
        logger.info("Executing auth method")
        from api_connector import ApiRequest # api_connector module imported from utils path to hit api and generate response
        api=ApiRequest(logger=logger)
        auth_response=api.post_request(api_url=config["auth_url"],headers=config["headers"],additional_post_parameters=config["additional_post_parameters"])
        token_type,auth_token=auth_response.json()["token_type"],auth_response.json()["access_token"]
        response=api.get_request(api_url=config["base_url"],headers={"Authorization":f"{token_type} {auth_token}"})
        if response.json()["result"]["status"]=="success":
            if len(response.json()["result"]["data"])>0:
                logger.info("Reponse Data has been generated")
                response_to_dataframe(response.json()["result"]["data"])
            else:
                logger.info("No Data to Fetch from Source")
        else:
            logger.warning("Error in fetching response Data ")
            raise
    except Exception as e:
        logger.error(f"Failed to execute auth method with error --> {e} {traceback.format_exc()}")
        raise

if __name__ == "__main__":
    curt_date = datetime.now()
    parser = argparse.ArgumentParser()
    parser.add_argument('--infile', nargs=1, help="JSON file to be processed", type=argparse.FileType('r'))
    arguments = parser.parse_args()
    config = json.load(arguments.infile[0])
    sys.path.insert(0,config['utils_path'])
    from utils import setup_logger, send_email_notification # utils module imported from utils path to setup logger and enable email alert function
    log_path = config["log_file_path"]
    log_filename = str(arguments.infile[0].name).split('/')[-1].replace('.json', '.log')
    log_filename=str(log_filename.replace(".log",f"_{curt_date.strftime('%Y_%m_%d_%H_%M_%S')}.log")) # Generates new log for every execution in the provided log path
    log_path = os.path.join(config["log_file_path"], log_filename)
    logger = setup_logger(log_path)
    logger.info("Ingestion Started")
    try:
        auth()
        send_email_notification(message=f"Ingestion Succesfull \n Script Path-> {os.path.abspath(__file__)} \n Config Path-> {arguments.infile[0].name}", subject=f"SUCCESS | {config['environment']} | {config.get('source','api')} Ingestion | {config['schema_name']}.{config['main_table']} | {config['redshift_profile']}",log_path=log_path,logger=logger)
        logger.info("Ingestion Completed")
    except Exception as e:
        logger.error(f"Exception occured {e} \n {traceback.format_exc()}")
        log_file_size= os.path.getsize(log_path)
        file_size=log_file_size / (1024 * 1024)
        if file_size < 1:
            send_email_notification(message=f"Script Path-> {os.path.abspath(__file__)} \n Config Path-> {arguments.infile[0].name} \n Exception -> {e} occured \n {traceback.format_exc()}", subject=f"FATAL | {config['environment']} | {config.get('source','api')} Ingestion | {config['schema_name']}.{config['main_table']} | {config['redshift_profile']}",log_path=log_path,logger=logger)
        else:
            send_email_notification(message=f"Script Path-> {os.path.abspath(__file__)} \n Config Path-> {arguments.infile[0].name} \n Log Path-> {log_path} \n Log File couldn't be uploaded due to size limit refer above log path for details \n Exception -> {e} occured \n {traceback.format_exc()}", subject=f"FATAL | {config['environment']} | {config.get('source','api')} Ingestion | {config['schema_name']}.{config['main_table']} | {config['redshift_profile']}",logger=logger)
