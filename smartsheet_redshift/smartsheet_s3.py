### Example Config File ###
"""{
    "source":"Source name for email alert",
    "environment":"Environment of the script location for email alert",
    "sheet_id":"SmartSheet ID If multiple sheets to be ingested to single table then provide all as comma seperated values",
    "url":"API Url to access SmartSheet",
    "auth_token":"Token to access api url",
    "ingestion_audit_field":"Name of the audit column that will be added in redshift table",
    "redshift_config":"Path where redshift credentials are stored",
    "s3_profile":"S3 profile",
    "s3_partition":"y/n if nothing provided by default it will not create any partition"
    "log_file":"Log folder path no need to mention file name",
    "replace_space_in_column_name":"If spaces in column names need to replaced or not provide y/n. If not provided by default will take it as n",
    "s3_bucket_name":"S3 Bucket Name",
    "data_orgin":"If data origin is not required provide n value for this key or by default it will create a data_orgin column to store sheet id as it's value"
    "s3_prefix_name":"Provide any one key and input as table name",
    "file_name":"If not provided in config by default it will be truncate_and_load",
    "file_type":""
    "posting_agent":"If posting agent is required provide the input values that will be stored in posting agent column"
    "utils_path":"path where utils py file is placed to import functions in t"
}"""

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

def dataframe_to_s3(df):
    """
    A method to gather data from the response received by hitting with column names and data

    Parameter:
    df (DataFrame)  : Response received by hitting API

    Returns:None
    """
    try:
        logger.info("Executing dataframe_to_redshift method")
        replace_space_in_column_name=config.get("replace_space_in_column_name","n")
        if replace_space_in_column_name.lower()=='y':
            df.columns=pd.Series(df.columns).replace(' ','_',regex=True).str.lower()
        partition=config.get("s3_partition",'n')
        s3=S3Operations(logger=logger, profile_name=config["s3_profile"], partition=partition)
        s3.upload_file(file_name=config["file_name"], file_type=config["file_type"], data=df, bucket=config["s3_bucket_name"], prefix=config["s3_prefix_name"],hour_partition=config.get('hour_partition',None))
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
            data_origin=config.get("data_orgin",'Y')
            if data_origin.lower()=='y':
                df["data_orgin"]=sheet
            audit_field=config.get("ingestion_audit_field",None)
            if audit_field:
                df[config["ingestion_audit_field"]]=datetime.today()
            logger.info("Data Fetched")
            main_df=pd.concat([main_df,df])
        dataframe_to_s3(main_df)
        send_email_notification(message=f"Ingestion Successful \n Script Path -> {os.path.abspath(__file__)} \n Config Path -> {arguments.infile[0].name}", subject=f"INFO - SUCCESS | {config['environment']} | {config.get('source','api')} Ingestion | SmartSheet ID - {config['sheet_id']} | {config['s3_bucket_name']}/{config['s3_prefix_name']}",log_path=log_path,logger=logger,add_on_email_stake_holders=config.get("add_on_email_stake_holders", None))
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
    from s3_operations import S3Operations
    log_filename = str(arguments.infile[0].name).split('/')[-1].replace('json', 'log')
    log_filename=str(log_filename.replace(".log",f"_{datetime.today().strftime('%Y_%m_%d_%H_%H_%S')}.log"))
    log_path=os.path.join(config["log_file"],log_filename)
    logger = setup_logger(log_path)
    try:
        logger.info("Ingestion Started")
        sys.exit(main())
    except Exception as e:
        logger.error(f"Exception occured-> {e}")
        log_file_size= os.path.getsize(log_path)
        file_size=log_file_size / (1024 * 1024)
        if file_size < 2:
            send_email_notification(message=f"Script Path -> {os.path.abspath(__file__)} \n Config Path -> {arguments.infile[0].name} \n Exception -> {e} \n {traceback.format_exc()}", subject=f"FATAL | {config['environment']} | {config.get('source','api')} Ingestion | SmartSheet ID - {config['sheet_id']} | {config['s3_bucket_name']}/{config['s3_prefix_name']}"
    ,log_path=log_path,logger=logger,add_on_email_stake_holders=config.get("add_on_email_stake_holders", None))
        else:
            send_email_notification(message=f"Script Path -> {os.path.abspath(__file__)} \n Config Path -> {arguments.infile[0].name} \n Couldnt attach Log file due to size limitations \n Log Path -> {log_path} \n Exception -> {e} \n {traceback.format_exc()}", subject=f"FATAL | {config['environment']} | {config.get('source','api')} Ingestion | SmartSheet ID - {config['sheet_id']} | {config['s3_bucket_name']}/{config['s3_prefix_name']}"
    ,logger=logger,add_on_email_stake_holders=config.get("add_on_email_stake_holders", None))

