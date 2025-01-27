import os
import sys
from datetime import datetime
import argparse
import json
import requests
import io
import pandas as pd
import traceback


def auth(url, headers):
    response = requests.get(url, headers)
    logger.info(f"Authentication response status code: {response.status_code}")
    return response


def main(config):
    logger.info("Script started")
    response = auth(config["url"], headers=config["headers"])

    if response.status_code != 200:
        logger.error(f"Failed to authenticate. Status code: {response.status_code}")
        return

    buffer_data = io.BytesIO(response.content)
    df = pd.read_excel(buffer_data, engine='openpyxl')
    logger.info(f"Dataframe loaded with {len(df)} rows and {len(df.columns)} columns")

    s3 = S3Operations(logger=logger, profile_name=config["s3_profile"], partition=config["s3_partition"])
    logger.info("S3 Operations initialized")

    try:
        s3.upload_file(
            file_name=config["file_name"],
            file_type=config["file_type"],
            data=df,
            bucket=config["s3_bucket_name"],
            prefix=config["s3_prefix_name"]
        )
        logger.info(
            f"File {config['file_name']} uploaded successfully to {config['s3_bucket_name']}/{config['s3_prefix_name']}")
    except Exception as e:
        logger.error(f"Error uploading file: {str(e)[:500]}")

    logger.info("Script execution finished")


if __name__ == "__main__":
    start_time = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    parser = argparse.ArgumentParser(description="Download data and save to a file.")
    parser.add_argument('--infile', required=True, help="JSON file with configuration", type=argparse.FileType('r'))
    args = parser.parse_args()
    config_file = args.infile
    config = json.load(config_file)

    sys.path.insert(0, config['utils_path'])
    from utils import setup_logger, send_email_notification
    from s3_operations import S3Operations

    log_filename = str(args.infile.name).split('/')[-1].replace('.json', '.log')
    log_path = os.path.join(config["log_file"], log_filename)
    logger = setup_logger(log_path)

    try:
        main(config)
        logger.info(f"Script finished at {datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}")
    except Exception as e:
        logger.error(f"Exception occurred -> {e}")
        send_email_notification(
            message=f"Exception -> {e} occurred at {os.path.abspath(__file__)} {traceback.format_exc()}, config path - {args.infile.name}",
            subject=f"FATAL | {config['environment']} | {config.get('source')} Ingestion | {config['file_name']} | {config['s3_bucket_name']} | {config['s3_profile']}",
            log_path=log_path,logger=logger)