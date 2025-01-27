import os,sys
from datetime import datetime
import argparse
import json
import requests
import io
import pandas as pd
import argparse, json, traceback
import argparse


def auth(url,headers):
    try:
        logger.info("Executing auth function")
        response = requests.get(url, headers)
        logger.info("Authentication Successful")
        return response
    except Exception as e:
        logger.error(f"Failed to execute auth function with error --> {e}")
        raise

def main(config):
    try:
        logger.info("Executing main function")
        response = auth(config["url"], headers=config["headers"])
        buffer_data = io.BytesIO(response.content)
        df = pd.read_excel(buffer_data, engine='openpyxl')
        if "s3_bucket_name" and "s3_prefix_name" in config:
            from s3_operations import S3Operations
            s3 = S3Operations(logger=logger, profile_name=config["s3_profile"], partition=config["s3_partition"])
            s3.upload_file(
            file_name=config["file_name"],
            file_type=config["file_type"],
            data=df,
            bucket=config["s3_bucket_name"],
            prefix=config["s3_prefix_name"])
            logger.info(f"Ingestion to S3 completed")
        if "main_table_name" and "schema_name" in config:
            df.columns=pd.Series(df.columns).replace(" ","_",regex=True).str.lower()
            df["ingestion_timestamp"]=datetime.today()
            from redshift_loader import Database
            Database(load_type=config.get("load_type", "truncate_and_load"), logger=logger, config=config["redshift_config"], profile=config["redshift_profile"], data=df, schema=config["schema_name"], main_table_name=config["main_table_name"])
            logger.info(f"Ingestion to Redshift Completed")
    except Exception as e:
        logger.error(f"Failed to execute main function with --> {e}")
        raise

if __name__ == "__main__":
    start_time= datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    parser = argparse.ArgumentParser(description="Download data and save to a file.")
    parser.add_argument('--infile', required=True, help="JSON file with configuration", type=argparse.FileType('r'))
    args = parser.parse_args()
    config_file = args.infile
    config = json.load(config_file)
    sys.path.insert(0, config['utils_path'])
    from utils import setup_logger, send_email_notification
    log_filename = str(args.infile.name).split('/')[-1].replace('.json', '.log')
    log_filename = str(log_filename.replace(".log",f"_{datetime.today().strftime('%Y_%m_%d_%H_%H_%S')}.log"))
    log_path = os.path.join(config["log_file_path"], log_filename)
    logger = setup_logger(log_path)
    logger.info("Ingestion Started")
    try:
        main(config)
        send_email_notification(message=f"Ingestion Completed \n Script Path-> {os.path.abspath(__file__)} \n Config Path-> {args.infile.name} \n Log Path-> {log_filename} ",subject=f"SUCCESS | {config['environment']} | {config.get('source')} Ingestion | {config['main_table_name']} | {config['redshift_profile']}", log_path=log_path, logger=logger)
        logger.info("Ingestion Completed")
    except Exception as e:
        logger.error(f"Exception occured-> {e} ")
        log_file_size= os.path.getsize(log_path)
        file_size=log_file_size / (1024 * 1024)
        if file_size < 1:
            logger.error(f"Exception occurred -> {e}")
            send_email_notification(message=f"Exception -> {e} \n Script Path-> {os.path.abspath(__file__)} \n Config Path-> {args.infile.name} \n Log Path-> {log_filename}  {traceback.format_exc()}",subject=f"FATAL | {config['environment']} | {config.get('source')} Ingestion | {config['main_table_name']} | {config['redshift_profile']}", log_path=log_path, logger=logger)
        else:
            send_email_notification(message=f"Exception -> {e} \n Script Path-> {os.path.abspath(__file__)} \n Config Path-> {args.infile.name} \n Log Path-> {log_filename} \n Couldn't attach log file due to file size limitation refer to log path mentioned  {traceback.format_exc()}",subject=f"FATAL | {config['environment']} | {config.get('source')} Ingestion | {config['main_table_name']} | {config['redshift_profile']}", logger=logger)
