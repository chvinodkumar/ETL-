import pandas as pd
import json
import requests
from datetime import date, datetime, timedelta
import argparse

curt_date = date.today()
pre_date = date.today() - timedelta(days=1)


def access_tok(config):
    token_response = requests.get(config["url_cred"], proxies=config.get("proxyDict", {}))
    logger.info(f"response status code: {token_response.status_code}")
    if token_response.status_code != 200:
        logger.error(f"Failed to response. Status code: {token_response.status_code}")
    access_token = token_response.json().get("access_token")
    return access_token

def get_program_data(config, access_token):
    offset = config["offset"]
    uqProgramIdList = set()
    json_res = []
    while offset >= 0:
        stroff = str(offset)
        url = config["url"].format(
            access_token=access_token,
            maxReturn=config["maxReturn"],
            offset=stroff,
            pre_date=pre_date.strftime("%Y-%m-%d"),
            curt_date=curt_date.strftime("%Y-%m-%d"))

        logger.info(f"printing URL link: {url}")

        response = requests.get(url, proxies=config.get("proxyDict", {}))

        # stop if out of scope
        if "No assets found for the given search criteria." in response.text:
            print(f"No more data. End offset (out of limit): {stroff}")
            break

        json_res.extend(response.json().get("result"))

        # get all programIDs
        for obj in response.json().get("result"):
            uqProgramIdList.add(obj.get("id"))
        offset += config["maxReturn"]

        # just to track progress
        if offset % 1000 == 0:
            print(f"Current offset: {stroff}")

    df = pd.json_normalize(json_res)
    logger.info(f"Dataframe loaded with {len(df)} rows and {len(df.columns)} columns")

    return df, uqProgramIdList


def upload_to_s3(s3, config, df):
    try:
        s3.upload_file(
            file_name=config["file_name"],
            file_type=config["file_type"],
            data=df,
            bucket=config["s3_bucket_name"],
            prefix=config["s3_prefix_name"])

        logger.info(f"File {config['file_name']} uploaded successfully to {config['s3_bucket_name']}/{config['s3_prefix_name']}")
    except Exception as e:
        logger.error(f"Error uploading file: {str(e)[:500]}")
        raise

def load_df_to_db(df, config):
    logger.info("Starting to load data into the database.")
    db_loader = Database(
        load_type=config.get("load_type", "incremental"),
        logger=logger,
        config=config["redshift_config"],
        profile=config["redshift_profile"],
        data=df,
        schema=config["schema_name"],
        main_table_name=config["main_table_name"])
    logger.info(f"Data loaded successfully into table: {config['main_table_name']}")


def main(config):
    logger.info("Main execution started")
    access_token = access_tok(config)
    get_program_data(config, access_token)
    df = get_program_data(config, access_token)
    s3 = S3Operations(logger=logger, profile_name=config["s3_profile"], partition=config["s3_partition"])
    logger.info("S3 Operations initialized")
    upload_to_s3(s3, config, df)
    load_df_to_db(df, config)
    logger.info("Main execution finished")

if __name__ == "__main__":
    start_time = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    parser = argparse.ArgumentParser(description="Download data and save to a file.")
    parser.add_argument('--infile', required=True, help="JSON file with configuration", type=argparse.FileType('r'))
    args = parser.parse_args()
    config = json.load(args.infile)
    sys.path.insert(0, config['utils_path'])
    from utils import setup_logger, send_email_notification
    from s3_operations import S3Operations
    from redshift_loader import Database
    log_filename = str(args.infile.name).split('/')[-1].replace('.json', '.log')
    log_path = os.path.join(config["log_file"], log_filename)
    logger = setup_logger(log_path)
    try:
        main(config)
        logger.info("END")
    except Exception as e:
        logger.error(f"Exception occurred -> {e}")
        send_email_notification()
