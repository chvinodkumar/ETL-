import pandas as pd
import json
import os
import requests
from datetime import date, datetime, timedelta
import argparse
import traceback


def access_tok(url, proxies):
    try:
        logger.info("Executing access_tok")
        token_response = requests.get(url=url, proxies=proxies)
        logger.info(f"response status code: {token_response.status_code}")
        if token_response.status_code == 200:
            access_token = token_response.json().get("access_token")
            logger.error("Authentication Successful")
            return access_token
        else:
            logger.error(f"Failed to authenticate {token_response.status_code} {token_response['message']}")
            raise Exception
    except Exception as e:
        logger.error(f"Failed to execute access_tok function with error --> {e} {traceback.format_exc()}")
        raise


def get_program_data(url, token):
    try:
        logger.info("Executing get_program_data function")
        offset = config["offset"]
        uqProgramIdList = set()
        json_res = []
        while offset >= 0:
            stroff = str(offset)
            url = url.format(
                access_token=token,
                maxReturn=config["maxReturn"],
                offset=stroff,
                pre_date=pre_date.strftime("%Y-%m-%d"),
                curt_date=curt_date.strftime("%Y-%m-%d"))
            logger.info(f"printing URL link: {url}")
            response = requests.get(url, proxies=config.get("proxyDict", {}))
            # stop if out of scope
            if "No assets found for the given search criteria." in response.text:
                logger.info(f"No more data. End offset (out of limit): {stroff}")
                break
            json_res.extend(response.json().get("result"))
            # get all programIDs
            for obj in response.json().get("result"):
                uqProgramIdList.add(obj.get("id"))
            offset += config["maxReturn"]
            # just to track progress
            if offset % 1000 == 0:
                logger.info(f"Current offset: {stroff}")
        df = pd.json_normalize(json_res)
        logger.info(f"Dataframe loaded with {len(df)} rows and {len(df.columns)} columns")
        return df
    except Exception as e:
        logger.error(f"Failed to execute get_program_data function with error --> {e} {traceback.format_exc()}")
        raise


def upload_to_s3(df):
    try:
        logger.info("Executing upload_to_s3 function")
        s3 = S3Operations(logger=logger, profile_name=config["s3_profile"], partition=config["s3_partition"])
        s3.upload_file(
            file_name=config["file_name"],
            file_type=config["file_type"],
            data=df,
            bucket=config["s3_bucket_name"],
            prefix=config["s3_prefix_name"])
        logger.info(
            f"File {config['file_name']} uploaded successfully to {config['s3_bucket_name']}/{config['s3_prefix_name']}")
    except Exception as e:
        logger.error(f"Failed to execute upload_to_s3 function with error --> {e} {traceback.format_exc()}")
        raise


def load_df_to_db(df):
    try:
        logger.info("Starting to load data into the database.")
        Database(load_type=config["load_type"], logger=logger, config=config["redshift_config"],
                 profile=config["redshift_profile"], data=df, schema=config["schema_name"],
                 main_table_name=config["main_table_name"])
        logger.info(f"Data loaded successfully into table: {config['main_table_name']}")
    except Exception as e:
        logger.error(f"Failed to execute load_df_to_db function with error --> {e} {traceback.format_exc()}")


def main():
    try:
        logger.info("Main execution started")
        access_token = access_tok(url=config["url_cred"], proxies=config.get("proxyDict", {}))
        df = get_program_data(url=config["url"], token=access_token)
        logger.info("S3 Operations initialized")
        upload_to_s3(df)
        load_df_to_db(df)
        logger.info("Ingestion Completed")
    except Exception as e:
        logger.error(f"Failed to execute main function with error --> {e} {traceback.format_exc()}")


if __name__ == "__main__":
    curt_date = date.today()
    pre_date = date.today() - timedelta(days=1)
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
    log_filename = str(log_filename.replace(".log", f"{curt_date.strftime('%Y_%m_%d_%H_%H_%S')}.log"))
    log_path = os.path.join(config["log_file"], log_filename)
    logger = setup_logger(log_path)
    try:
        logger.info("Ingestion Started")
        main()
        send_email_notification(
            message=f"Ingestion Succesfull \n Script Path-> {os.path.abspath(__file__)} \n Config Path-> {args.infile[0].name}",
            subject=f"SUCCESS | {config['environment']} | {config.get('source', 'api')} Ingestion | Marketo API | {config['schema_name']}.{config['table_name']} {config['redshift_profile']}",
            log_path=log_path, add_on_email_stake_holders=config.get("add_on_email_stake_holders", None))
    except Exception as e:
        logger.error(f"Exception occurred -> {e} ")
        log_file_size = os.path.getsize(log_path)
        file_size = log_file_size / (1024 * 1024)
        if file_size < 1:
            send_email_notification(
                message=f"Script Path-> {os.path.abspath(__file__)} \n Config Path-> {args.infile[0].name} \n Exception -> {e} occured \n {traceback.format_exc()}",
                subject=f"FATAL | {config['environment']} | {config.get('source', 'api')} Ingestion | Marketo API | {config['schema_name']}.{config['table_name']} {config['redshift_profile']}",
                log_path=log_path, add_on_email_stake_holders=config.get("add_on_email_stake_holders", None))
        else:
            send_email_notification(
                message=f"Script Path-> {os.path.abspath(__file__)} \n Config Path-> {args.infile[0].name} \n Log Path-> {log_path} \n Log File couldn't be uploaded due to size limit refer above log path for details \n Exception -> {e} occured \n {traceback.format_exc()}",
                subject=f"FATAL | {config['environment']} | {config.get('source', 'api')} Ingestion | Marketo API| {config['schema_name']}.{config['table_name']} {config['redshift_profile']}",
                add_on_email_stake_holders=config.get("add_on_email_stake_holders", None))