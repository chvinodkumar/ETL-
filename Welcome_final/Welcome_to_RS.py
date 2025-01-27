import os,sys
from datetime import datetime
import argparse
import json
import requests
import io
import pandas as pd
import argparse, json, traceback
from io import BytesIO
import logging
import argparse
import configparser
from sqlalchemy import create_engine


def auth(url,headers):
    response = requests.get(url, headers)
    return response

def main(config):
    logger.info("Executing main function")
    response = auth(config["url"], headers=config["headers"])
    buffer_data = io.BytesIO(response.content)
    df = pd.read_excel(buffer_data, engine='openpyxl')
    print(df["Start Date"])
    df.columns=pd.Series(df.columns).replace(" ","_",regex=True).str.lower()
    df["ingestion_timestamp"]=datetime.today()
    print(df.columns)
    Database(load_type=config.get("load_type", "truncate_and_load"), logger=logger, config=config["redshift_config"], profile=config["redshift_profile"], data=df, schema=config["schema_name"], main_table_name=config["main_table_name"])
    print("test")

if __name__ == "__main__":
    start_time= datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    parser = argparse.ArgumentParser(description="Download data and save to a file.")
    parser.add_argument('--infile', required=True, help="JSON file with configuration", type=argparse.FileType('r'))
    args = parser.parse_args()
    config_file = args.infile
    config = json.load(config_file)
    sys.path.insert(0, config['utils_path'])
    from utils import setup_logger, send_email_notification
    from redshift_loader import Database
    log_filename = str(args.infile.name).split('/')[-1].replace('.json', '.log')
    log_path = os.path.join(config["log_file"], log_filename)
    logger = setup_logger(log_path)
    try:
        main(config)
        logger.info(f"Script finished at {datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}")
    except Exception as e:
        logger.error(f"Exception occurred -> {e}")
        send_email_notification(
            message=f"Exception -> {e} occured at {os.path.abspath(__file__)} {traceback.format_exc()},config path - {args.infile.name}",
            subject=f"FATAL | {config['environment']} | {config.get('source')} Ingestion | {config['main_table_name']} | {config['redshift_profile']}"
            , log_path=log_path, logger=logger)


