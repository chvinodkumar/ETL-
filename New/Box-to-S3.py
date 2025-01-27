### Example Config File ###

"""{

    "source":"Source name for email alert",
    "environment":"Environment of the script location for email alert ",
    "box_config_path":"Path where box credentials are stored"
    "box_id":"provide the box source id",
    "s3_profile":"s3 profile",
    "log_file":"Log folder path no need to mention file name",
    "utils_path":"path where utils py file is placed to import functions in t",
    "s3_partition" : "is partiotion required or not y or n",
    "s3_prefix" : "need to provide the s3 prefix",
    "file_name": " need to provid the file name"

    }"""




#### Importing Necessary Packages ####
import sys
from datetime import datetime
import os
from io import BytesIO, StringIO
from boxsdk import JWTAuth, Client
import pandas as pd
import boto3
import argparse
import json

def box_access(JWT_file_path):
    """
    A method to authenticate with the box

    Parameter:
    JWT_file_path (str) : Box config path

    Returns:
    client (Object)     : Client object to access box files

    """

    auth = JWTAuth.from_settings_file(JWT_file_path)
    client = Client(auth)
    logger.info("Box authentication has been established successfully")
    return client

def box_s3(client, folder_id):
    """
    A method to download files from box and upload files into s3

    Parameter:
    client (object) : Client object which will be used to access box folder
    folder_id (str) : box folder id

    Returns:None
    """

    for item in client.folder(folder_id).get_items():
        if item.type == "file" and item.name == config["file_name"]:
            buffer = BytesIO()
            client.file(item.id).download_to(buffer)
            buffer.seek(0)
            if config["s3_partition"] == "Y":
                obj = config["s3_prefix"] + f"/year={year}/month={month}/day={day}/" + config["file_name"]
            else:
                obj = config["s3_prefix"] +"/"+ config["file_name"]
            session = boto3.Session(profile_name=config["s3_profile"])
            s3 = session.client('s3')
            s3.upload_fileobj(buffer,config["bucket_name"],obj)
            logger.info(f"{config['file_name']} uploaded to S3 successfully")
            break  # Exit loop after finding the file
    else:
        logger.error(f"File '{config['file_name']}' not found in Box folder with ID '{folder_id}'")

def main():
    """
    A method to call other methods accordingly to proceed with ingestion
    """

    client = box_access(config["box_config_path"])
    box_s3(client, config["box_id"])
    logger.info("Ingestion Completed")

if __name__ == "__main__":
    start = datetime.today().strftime("%Y-%m-%d_%H:%M:%S")
    year, month, day = datetime.today().strftime("%Y"), datetime.today().strftime("%m"), datetime.today().strftime("%d")
    parser = argparse.ArgumentParser()
    parser.add_argument('--infile', nargs=1, help="JSON file to be processed", type=argparse.FileType('r'))
    arguments = parser.parse_args()
    config = json.load(arguments.infile[0])
    parent_path = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(config["utils_path"])
    from utils import setup_logger, send_email_notification
    log_filename = str(arguments.infile[0].name).split('/')[-1].replace('json', 'log')
    logger = setup_logger(os.path.join(config["log_file"], log_filename))
    try:
        logger.info("Ingestion Started")
        sys.exit(main())
    except Exception as e:
        logger.error(f"Exception occurred: {e}")
        send_email_notification(message=f"Exception -> {e} occured at {parent_path}", subject=f"{config['source']} Ingestion Failure | {config['environment']}",log_path=log_filename)
==================

'''{
    "source":"box",
    "environment":"production",
    "box_config_path":"/root/.aws/conf.json",
    "file_name":"Excess_DC_Transit_Packs.csv",
    "utils_path":"/data-ingestion/custom_scripts/global/",
    "box_id":"228944463693",
    "s3_partition":"",
    "s3_prefix":"Ingestion/ISC",
    "bucket_name":"odp-us-prod-hc-pdx",
    "log_file":"/data-ingestion/custom_scripts/isc/log",
    "s3_profile":"prod"
}
'''

'''==================
######################################################
#Name: Kola Devi Revanth
#Date: 29-07-2024
#Version : 1.0
#Version Comments: Initial Version
#Objective: Box to S3 ingestion
#userstory:
########################################################

### Example Config File ###

"""{

    "source":"Source name for email alert",
    "environment":"Environment of the script location for email alert ",
    "box_config_path":"Path where box credentials are stored"
    "box_id":"provide the box source id",
    "s3_profile":"s3 profile",
    "log_file":"Log folder path no need to mention file name",
    "utils_path":"path where utils py file is placed to import functions in t",
    "s3_partition" : "is partiotion required or not y or n",
    "s3_prefix" : "need to provide the s3 prefix",
    "file_name": " need to provid the file name"

    }"""




#### Importing Necessary Packages ####
import sys
from datetime import datetime
import os
from io import BytesIO, StringIO
from boxsdk import JWTAuth, Client
import pandas as pd
import boto3
import argparse
import json

def box_access(JWT_file_path):
    """
    A method to authenticate with the box

    Parameter:
    JWT_file_path (str) : Box config path

    Returns:
    client (Object)     : Client object to access box files

    """

    auth = JWTAuth.from_settings_file(JWT_file_path)
    client = Client(auth)
    logger.info("Box authentication has been established successfully")
    return client

def box_s3(client, folder_id):
    """
    A method to download files from box and upload files into s3

    Parameter:
    client (object) : Client object which will be used to access box folder
    folder_id (str) : box folder id

    Returns:None
    """

    for item in client.folder(folder_id).get_items():
        if item.type == "file" and item.name == config["file_name"]:
            buffer = BytesIO()
            client.file(item.id).download_to(buffer)
            buffer.seek(0)
            if config["s3_partition"] == "Y":
                obj = config["s3_prefix"] + f"/year={year}/month={month}/day={day}/" + config["file_name"]
            else:
                obj = config["s3_prefix"] +"/"+ config["file_name"]
            session = boto3.Session(profile_name=config["s3_profile"])
            s3 = session.client('s3')
            s3.upload_fileobj(buffer,config["bucket_name"],obj)
            logger.info(f"{config['file_name']} uploaded to S3 successfully")
            break  # Exit loop after finding the file
    else:
        logger.error(f"File '{config['file_name']}' not found in Box folder with ID '{folder_id}'")

def main():
    """
    A method to call other methods accordingly to proceed with ingestion
    """

    client = box_access(config["box_config_path"])
    box_s3(client, config["box_id"])
    logger.info("Ingestion Completed")

if __name__ == "__main__":
    start = datetime.today().strftime("%Y-%m-%d_%H:%M:%S")
    year, month, day = datetime.today().strftime("%Y"), datetime.today().strftime("%m"), datetime.today().strftime("%d")
    parser = argparse.ArgumentParser()
    parser.add_argument('--infile', nargs=1, help="JSON file to be processed", type=argparse.FileType('r'))
    arguments = parser.parse_args()
    config = json.load(arguments.infile[0])
    parent_path = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(config["utils_path"])
    from utils import setup_logger, send_email_notification
    log_filename = str(arguments.infile[0].name).split('/')[-1].replace('json', 'log')
    logger = setup_logger(os.path.join(config["log_file"], log_filename))
    try:
        logger.info("Ingestion Started")
        sys.exit(main())
    except Exception as e:
        logger.error(f"Exception occurred: {e}")
        send_email_notification(message=f"Exception -> {e} occured at {parent_path}", subject=f"{config['source']} Ingestion Failure | {config['environment']}",log_path=log_filename)

'''