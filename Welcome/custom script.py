#######################################################
# Name: Kola Devi Revanth
# Date: 13-08-2024
# Version : 1.0
# Version Comments: Initial Version
# Objective: Fetching response from Google BigQuery Api with necessary inputs and ingesting desired type to file to S3
# userstory:
########################################################

#### Sample Config File ####
"""{
    "source":"",
    "environment":"",
    "log_file":"",
    "utils_path":"",
    "application_credentials":"",
    "project_id":"",
    "query":"",
    "s3_bucket_name":"",
    "s3_prefix_name":"",
    "data_path":"",
    "s3_profile":"",
    "s3_partition":"",
    "required_datatypes":"",
    "file_name":"",
    "file_type":"",
    "column_mappings" :""
}"""

#### Importing Necessary Packages ####
from datetime import datetime, timedelta
import pandas as pd
import os, sys
from google.cloud import bigquery
from google.oauth2 import service_account
import argparse, json, traceback


def data(response):
    """Constructs a DataFrame with the response provided as input, performs column mapping if provided,
    and initiates S3 module to insert the data to S3.

    Parameter:
    response (json) : Response fetched from API

    Returns: None
    """
    try:
        logger.info("Executing data function")
        df = pd.DataFrame()
        mapping = config.get("column_mappings", None)
        if mapping:
            data = {key: [] for key in mapping.keys()}
            for item in response:
                for key in mapping.keys():
                    data[key].append(item.get(key, None))
            logger.info("Columns mapping successful")
            df = pd.DataFrame(data)
        else:
            logger.info("No column mapping provided, using raw response to construct DataFrame")
            df = pd.DataFrame(response)
        if not df.empty:
            df.dropna(how='all', inplace=True)
            s3 = S3Operations(logger=logger, profile_name=config["s3_profile"], partition=config["s3_partition"])
            s3.upload_file(
                file_name=config["file_name"],
                file_type=config["file_type"],
                data=df,
                bucket=config["s3_bucket_name"],
                prefix=config["s3_prefix_name"],
                dtypes=config.get("required_datatypes", None),
                data_path=config["data_path"]
            )
        else:
            logger.info("No data fetched from source")
            send_email_notification(
                message=f"No Data to Fetch from Source execution successful ,code path-> {os.path.abspath(__file__)} ,config path - {arguments.infile[0].name}",
                subject=f"INFO | {config['environment']} | {config.get('source', 'api')} Ingestion | {config['s3_bucket_name']}{config['s3_bucket_name']}"
                , log_path=log_path, logger=logger)
            sys.exit(1)
    except Exception as e:
        logger.error(f"Failed to construct DataFrame from response fetched with error --> {e} {traceback.format_exc()}")
        raise


def main():
    """Establishes connection to Google BigQuery API using query provied

    Parameter:None

    Returns:None
    """
    try:
        logger.info("Executing main function")
        if "application_credentials" in config:
            logger.info("Google application credentials found setting environment")
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config["application_credentials"]
            logger.info("Environment set for Google Application")
            credentials = service_account.Credentials.from_service_account_file(config["application_credentials"])
            logger.info("Credentails fetched")
            client = bigquery.Client(credentials=credentials, project=config["project_id"])
            logger.info("Authentication sucessfull")
            gcp_query = (f'{config["query"]}"{past_date}"')
            query_job = client.query(gcp_query)
            rows = query_job.result()
            logger.info("Query ran successfully, response has been fetched")
            data(rows)
            logger.info("Ingestion Succesful")
        send_email_notification(message=f"Ingestion Sucessful ,config path - {arguments.infile[0].name}",
                                subject=f"INFO - SUCCESS | {config['environment']} | {config.get('source', 'api')} Ingestion | {config['s3_bucket_name']}{config['s3_prefix_name']}",
                                log_path=log_path, logger=logger)
    except Exception as e:
        logger.error(f"Failed to execute main function with error --> {e} {traceback.format_exc()}")
        raise


if __name__ == "__main__":
    now = datetime.now() - timedelta(1)
    past_date = datetime.strftime(now, '%Y-%m-%d')
    parser = argparse.ArgumentParser()
    parser.add_argument('--infile', nargs=1, help="JSON file to be processed", type=argparse.FileType('r'))
    arguments = parser.parse_args()
    config = json.load(arguments.infile[0])
    sys.path.insert(0, config['utils_path'])
    from utils import setup_logger, send_email_notification
    from s3_operations import S3Operations

    log_filename = str(arguments.infile[0].name).split('/')[-1].replace('json', 'log')
    log_path = os.path.join(config["log_file"], log_filename)
    logger = setup_logger(log_path)
    try:
        logger.info("Ingestion Started")
        sys.exit(main())
    except Exception as e:
        logger.error(f"Exception occured-> {e}")
        send_email_notification(
            message=f"Exception -> {e} occured at {os.path.abspath(__file__)} {traceback.format_exc()},config path - {arguments.infile[0].name}",
            subject=f"FATAL | {config['environment']} | {config.get('source', 'api')} Ingestion | {config['sheet_id']} | {config['s3_bucket_name']}{config['s3_bucket_name']}"
            , log_path=log_path, logger=logger)

