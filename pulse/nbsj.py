########### Importing Packages ###############
import requests,json
import pandas as pd
import argparse
import os,sys
from datetime import datetime
import traceback

def auth(id: str, token: str) -> requests.Response:
    """
    A method to hit API and fetch the response.

    Parameters:
    id (str)    : Sheet ID to fetch response from via API
    token (str) : Authentication Token used to hit the API

    Returns:
    response (requests.Response) : The response object from the API
    """
    try:
        logger.info("Executing auth method")
        headers = { "Authorization": f"Bearer {token}" }
        payload = {}
        url = f"{config['url']}{id}"
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            logger.info("Authentication Successfull")
            return response
        else:
            logger.info("Authentication Failed")
            try:
                error_message = response.json().get("message", "No message provided")
            except ValueError:
                error_message = "No valid JSON response"
            error_info = f"Authentication failed. Status code: {response.status_code}. Message: {error_message}"
            logger.error(error_info)
            raise Exception(error_info)
    except Exception as e:
        logger.error(f"Failed to execute auth method , error --> {e} {traceback.format_exc()}")
        raise

def data(response):
    """
    A method to gather data from the response received by hitting with column names and data

    Parameter:
    response (str) : Response received by hitting API

    Returns:
    records (List) : List of values as column:Value
    """
    try:
        logger.info("Executing data method")
        data_cols = response.json()['columns']
        data_rows = response.json()['rows']
        columns={}
        for item in data_cols:
            columns.update({item['id']:item['title']})
        records=[]
        for row in data_rows:
            record={}
            for cell in row['cells']:
                if "value" in cell:
                    record.update({columns[cell['columnId']]:cell['value']})
                else:
                    record.update({columns[cell['columnId']]:''})
            records.append(record)
        logger.info("Records Fetched")
        return records
    except Exception as e:
        logger.error(f"Failed to execute data method in data method , error --> {e} {traceback.format_exc()}")
        raise

def upload_touch_file():
    try:
        from s3_operations import S3Operations
        file_name=config["touch_file_name"]
        file_name=str(file_name).replace(".csv",f"-{datetime.now().strftime('%Y_%m_%d-%H_%M')}.csv")
        s3=S3Operations(logger=logger, profile_name=config["touch_file_s3_profile"], partition=config["touch_file_partition"])
        s3.upload_file(file_name=file_name, file_type=config["touch_file_type"], data=pd.DataFrame(), bucket=config["touch_file_s3_bucket"], prefix=config["touch_file_s3_prefix"])
        logger.info(f"Touch File created - {config['touch_file_s3_bucket']}/{config['touch_file_s3_prefix']}{file_name}")
    except Exception as e:
        logger.error(f"Failed to execute upload_touch_file method, error --> {e} {traceback.format_exc()}")
        raise

def dataframe_to_redshift(df):
    """
    A method to gather data from the response received by hitting with column names and data

    Parameter:
    df (DataFrame)  : Response received by hitting API

    Returns:None
    """
    try:
        logger.info("Executing dataframe_to_redshift method")
        df['DAT_ORGN']=table_name
        df['POSTNG_AGNT']='python'
        df['EDW_POSTNG_TS']=datetime.today()
        df['EDW_UPD_TS']=datetime.today()
        df.columns=pd.Series(df.columns).str.lower()
        Database(load_type=config.get("load_type","truncate_and_load"),logger=logger,config=config["redshift_config"],profile=config["redshift_profile"],data=df,schema=config["schema_name"],main_table_name=table_name,stage_table_name=config.get("stage_table",None),primary_key=config.get("primary_key",None))
        if "touch_file_s3_bucket" and "touch_file_s3_prefix" in config:
            upload_touch_file()
    except Exception as e:
        logger.error(f"Failed to execute main method in dataframe_to_redshift method , error --> {e} {traceback.format_exc()}")
        raise

def main():
    """
    A method to call other function accordingly to execute ingestion steps

    Parameter:None

    Returns:None
    """
    try:
        logger.info("Executing main method")
        main_df=pd.DataFrame()
        for sheet in config["sheet_id"].split(','):
            api_response=auth(sheet,config["auth_token"])
            df = pd.DataFrame(data(api_response))
            logger.info("Data Fetched")
            main_df=pd.concat([main_df,df])
        dataframe_to_redshift(main_df)
        send_email_notification(message=f"Ingestion Sucessfull \n Script Path-> {os.path.abspath(__file__)} \n Config Path-> {arguments.infile[0].name}", subject=f"INFO - SUCCESS | {config['environment']} | {config.get('source','api')} Ingestion | SmartSheet ID - {config['sheet_id']} | {config['schema_name']}.{table_name} {config['redshift_profile']}",log_path=log_path,logger=logger,add_on_email_stake_holders=config.get("add_on_email_stake_holders", None))
        logger.info("Ingestion Completed")
    except Exception as e:
        logger.error(f"Failed to execute main method , error --> {e} {traceback.format_exc()}")
        raise

if __name__=="__main__":
    year, month, day = datetime.today().strftime("%Y"), datetime.today().strftime("%m"), datetime.today().strftime("%d")
    parser = argparse.ArgumentParser()
    parser.add_argument('--infile', nargs=1, help="JSON file to be processed", type=argparse.FileType('r'))
    arguments = parser.parse_args()
    config = json.load(arguments.infile[0])
    utils_path=config["utils_path"]
    if 'utils' not in utils_path:
        utils_path=os.path.join(utils_path,"utils/")
    sys.path.insert(0,utils_path)
    from utils import setup_logger, send_email_notification
    from redshift_loader import Database
    log_filename = str(arguments.infile[0].name).split('/')[-1].replace('json', 'log')
    log_filename=str(log_filename.replace(".log",f"_{datetime.today().strftime('%Y_%m_%d_%H_%H_%S')}.log"))
    log_path=os.path.join(config["log_file"],log_filename)
    logger = setup_logger(log_path)
    table_name=config.get("main_table")
    if not table_name:table_name=config["table_name"]
    try:
        logger.info("Ingestion Started")
        sys.exit(main())
    except Exception as e:
        logger.error(f"Exception occured-> {e}")
        log_file_size= os.path.getsize(log_path)
        file_size=log_file_size / (1024 * 1024)
        if file_size < 1:
            send_email_notification(message=f"Script Path-> {os.path.abspath(__file__)} \n Config Path-> {arguments.infile[0].name} \n Exception -> {e} occured \n {traceback.format_exc()}", subject=f"FATAL | {config['environment']} | {config.get('source','api')} Ingestion | SmartSheet ID - {config['sheet_id']} | {config['schema_name']}.{table_name} {config['redshift_profile']}",log_path=log_path,logger=logger,add_on_email_stake_holders=config.get("add_on_email_stake_holders", None))
        else:
            send_email_notification(message=f"Script Path-> {os.path.abspath(__file__)} \n Config Path-> {arguments.infile[0].name} \n Exception -> {e} occured \n Log File couldn't be attached with this mail due to file size limit being exceeded, Log Path-> {log_path} \n {traceback.format_exc()}", subject=f"FATAL | {config['environment']} | {config.get('source','api')} Ingestion | SmartSheet ID - {config['sheet_id']} | {config['schema_name']}.{table_name} {config['redshift_profile']}",logger=logger,add_on_email_stake_holders=config.get("add_on_email_stake_holders", None))