import os
import sys
import json
import logging
import argparse
import requests
import io
import pandas as pd
import traceback
from datetime import datetime


def auth(url, headers):
    """Authentication and return the response"""
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        logger.error(f"Authentication failed with status code: {response.status_code}")
    return response
def load_data_to_dataframe(response):
    """Load data from the response content into a pandas DataFrame"""
    buffer_data = io.BytesIO(response.content)
    df = pd.read_excel(buffer_data, engine='openpyxl')
    logger.info(f"DataFrame loaded with {len(df)} rows and {len(df.columns)} columns")
    return df
def dataframe(df):
    """Prepare the DataFrame by renaming columns and adding ingestion timestamp"""
    df.columns = pd.Series(df.columns).replace(" ", "_", regex=True).str.lower()
    df[config["column_add"]] = datetime.today()
    logger.info(f"DataFrame columns after preparation: {df.columns.tolist()}")
    return df

def load_df_to_db(df, config):
    try:
        logger.info("Starting to load data into the database.")
        db_loader = Database(
            load_type=config.get("load_type", "truncate_and_load"),
            logger=logger,
            config=config["redshift_config"],
            profile=config["redshift_profile"],
            data=df,
            schema=config["schema_name"],
            main_table_name=config["main_table_name"])
        logger.info(f"Data loaded successfully into table: {config['main_table_name']}")
    except Exception as e:
        logger.error(f"Failed to load data into the database: {e}")
        raise


def main(config):
    """Main execution function"""
    logger.info("Executing main function")
    response = auth(config["url"], headers=config["headers"])
    df = load_data_to_dataframe(response)
    df = dataframe(df)
    load_df_to_db(df, config)
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
            message=f"Exception -> {e} occurred at {os.path.abspath(__file__)} {traceback.format_exc()}, config path - {args.infile.name}",
            subject=f"FATAL | {config.get('environment')} | {config.get('source')} Ingestion | {config['main_table_name']} | {config['redshift_profile']}",
            log_path=log_path,logger=logger)
