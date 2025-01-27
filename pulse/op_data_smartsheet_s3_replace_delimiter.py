########### Importing Packages ###############
import requests,json
import pandas as pd
import argparse
import os,sys
from datetime import datetime
import traceback
from io import StringIO,BytesIO

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
        headers = { "Authorization": f"Bearer {token}","Accept": "text/csv" }
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

def main():
    """
    A method to call other function accordingly to execute ingestion steps

    Parameter:None

    Returns:None
    """
    try:
        logger.info("Executing main method")
        api_response=auth(config["sheet_id"],config["auth_token"])
        sheet = api_response.content.decode('utf-8')
        csv_buffer = sheet.replace(',', ';')
        data = BytesIO(csv_buffer.encode('utf-8'))
        from s3_connector import S3Connector
        s3=S3Connector(logger=logger, profile_name=config["s3_profile"])
        object_name = config['s3_prefix_name'] + f"year={datetime.today().strftime('%Y')}/month={datetime.today().strftime('%m')}/day={datetime.today().strftime('%d')}/"
        s3.s3_client.upload_fileobj(data, config["s3_bucket_name"], object_name+config["file_name"])
        send_email_notification(message=f"Ingestion Sucessfull \n Script Path-> {os.path.abspath(__file__)} \n Config Path-> {arguments.infile[0].name}", subject=f"INFO - SUCCESS | {config['environment']} | {config.get('source','api')} Ingestion | SmartSheet ID - {config['sheet_id']} | {config['s3_bucket_name']}/{config['s3_prefix_name']} | {config['s3_profile']}",log_path=log_path,logger=logger,add_on_email_stake_holders=config.get("add_on_email_stake_holders", None))
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


