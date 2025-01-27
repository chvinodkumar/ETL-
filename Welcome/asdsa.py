import os,sys
from datetime import datetime
import argparse
import json
import requests
import io
import pandas as pd
import argparse, json, traceback

def auth(url,headers):
    response = requests.get(url, headers)
    return response

def main(config):
    logger.info("Executing main function")
    response = auth(config["url"], headers=config["headers"])
    buffer_data = io.BytesIO(response.content)
    df = pd.read_excel(buffer_data, engine='openpyxl')

    s3 = S3Operations(logger=logger, profile_name=config["s3_profile"], partition=config["s3_partition"])
    print("test")
    s3.upload_file(file_name=config["file_name"],file_type=config["file_type"],data=df,bucket=config["s3_bucket_name"],prefix=config["s3_prefix_name"])
    print("test2")

if __name__ == "__main__":
    start_time= datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    parser = argparse.ArgumentParser(description="Download data and save to a file.")
    parser.add_argument('--infile', required=True, help="JSON file with configuration", type=argparse.FileType('r'))
    args = parser.parse_args()
    config_file = args.infile
    config = json.load(config_file)
    sys.path.insert(0, config['utils_path'])
    from utils import setup_logger, send_email_notification
    from s3_operations import S3Operations
    from red import Database
    log_filename = str(args.infile.name).split('/')[-1].replace('.json', '.log')
    log_path = os.path.join(config["log_file"], log_filename)
    logger = setup_logger(log_path)
    try:
        main(config)
        logger.info(f"Script finished at {datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}")
    except Exception as e:
        logger.error(f"Exception occured-> {e}")
        send_email_notification(
            message=f"Exception -> {e} occured at {os.path.abspath(__file__)} {traceback.format_exc()},config path - {args.infile.name}",
            subject=f"FATAL | {config['environment']} | {config.get('source')} Ingestion | {config['file_name']} | {config['s3_bucket_name']}{config['s3_profile']}"
            , log_path=log_path, logger=logger)


