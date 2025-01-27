####Importing Necessary Packages####
import argparse
from datetime import datetime
import sys,os
import json
import pandas as pd
import requests
import traceback
from datetime import datetime

def fetch_data_api(address):
    """
    Fetches latitude and longitude for a given address from the Google Maps API.

    Parameters:
        address (str): The address to geocode.

    Returns:
        tuple: A dictionary containing 'lat' and 'lng' values, and an error message (if any).

    Raises:
        Exception: If the API call fails or returns unexpected data.
    """
    try:
        logger.info("Executing fetch_data_api method")
        url=config["url"].format(address=address)
        from api_connector import ApiRequest # api_connector Module imported for hitting API
        api=ApiRequest(logger=logger)
        api.get_request(api_url=url)
        response = requests.get(url)
        result = response.json().get('results',[])
        if len(result)>0 and response.json()["status"]=="OK":
            for res in result:
                geo=res["geometry"]["location"]
                return geo ,None
        else:
            error_message=f"{response.json()['status']} Latitude and Longitude not found for {config['primary_key']}"
            return {"lat":-10,"lng":-10},error_message
    except Exception as e:
        logger.error(f"Failed to execute fetch_data_api method, error --> {e} {traceback.format_exc()}")
        raise

def fetch_matching_file_names():
    """
    Fetches the latest file names from an S3 bucket based on a predefined pattern.

    Returns:
        files (list): A list of file names matching the specified pattern.

    Raises:
        Exception: If fetching the file names fails.
    """
    try:
        from indirect_file_names_fetcher import pattern_files_loader
        file_pattern=pattern_files_loader(logger=logger,file_pattern=config["file_pattern"],s3_profile=config["s3_profile"],bucket=config["s3_bucket_name"],bucket_prefix=config["s3_prefix_name"])
        files=file_pattern.get_s3_patternfiles()
        return files
    except Exception as e:
        logger.error(f"Failed to execute fetch_latest_file_name method, error --> {e} {traceback.format_exc()}")
        raise

def get_data_from_s3_ingest_redshift():
    """
    Retrieves data from S3, enriches it using Google Maps API, and ingests it into Redshift.

    Actions:
        - Retrieves files from S3 based on a specified pattern.
        - Removes already processed files.
        - Processes new files by enriching them with Google Maps API data.
        - Ingests the enriched data into Redshift.

    Raises:
        Exception: If any operation (S3, API, or database connection, insert) fails.
    """
    try:
        logger.info("Executing retrive_data_s3 method")
        if "s3_profile" in config:
            file_names = fetch_matching_file_names()
            from indirect_file_names_db_operations import db_operations
            audit_db=db_operations(logger=logger,file_pattern=config["file_pattern"],source_system_type=config["source"],source_system=config["source"],source_system_path=config["s3_bucket_name"]+config["s3_prefix_name"],target=f'{config["schema_name"]}.{config["main_table"]}',pattern_load_type=config["load_type"])
            to_be_processed_files=audit_db.remove_already_processed_files(list_filenames=file_names)
            print(to_be_processed_files)
            run_time_audit=audit_db.generate_insert_table_dataframe(items=to_be_processed_files)
            audit_db.insert_records(df=run_time_audit)
            for process_file in to_be_processed_files:
                from s3_operations import S3Operations # s3_operations Module imported for S3 operations
                s3 = S3Operations(logger=logger, profile_name=config["s3_profile"])
                key=config["s3_prefix_name"]+process_file
                df = s3.getobject_s3(file_type=config["file_type"], bucket_name=config["s3_bucket_name"], key=key)
                address = df["address"]
                df["error_message"]=None
                for index,data in df.iterrows():
                    address = data["address"]
                    location,error_message = fetch_data_api(address=address)
                    if location:
                        df.at[index,"latitude"]=location["lat"] # updates the particular column in the row with value generated from API
                        df.at[index,"longitude"]=location["lng"] # updates the particular column in the row with value generated from API
                        if error_message:df.at[index,"error_message"]=error_message
                if not df.empty:
                    df["ingestion_timestamp"]=datetime.today()
                    from redshift_loader import Database # redshift_loader Module imported for Database operations
                    Database(load_type=config["load_type"],logger=logger,config=config["redshift_config"],profile=config["redshift_profile"],data=df,schema=config["schema_name"],main_table_name=config["main_table"],stage_table_name=config.get("stage_table",None),primary_key=config.get("primary_key",None))
                    audit_db.move_data(filename=process_file)
                else:logger.info("No data to insert")
        else:raise ValueError("Missing Value s3_profile in config provided")
    except Exception as e:
        logger.error(f"Failed to execute retrive_data_s3 method, error --> {e} {traceback.format_exc()}")
        raise

if __name__ == "__main__":
    curt_date = datetime.now()
    parser = argparse.ArgumentParser()
    parser.add_argument('--infile', nargs=1, help="JSON file to be processed", type=argparse.FileType('r'))
    arguments = parser.parse_args()
    config = json.load(arguments.infile[0])
    sys.path.insert(0,config['utils_path'])
    from utils import setup_logger, send_email_notification
    log_path = config["log_file_path"]
    log_filename = str(arguments.infile[0].name).split('/')[-1].replace('.json', '.log')
    log_filename=str(log_filename.replace(".log",f"_{curt_date.strftime('%Y_%m_%d_%H_%M_%S')}.log"))
    log_path = os.path.join(config["log_file_path"], log_filename)
    logger = setup_logger(log_path)
    try:
        logger.info("Ingestion Started")
        get_data_from_s3_ingest_redshift()
        logger.info("Ingestion Completed")
        send_email_notification(message=f"Ingestion Sucessfull \n Script Path-> {os.path.abspath(__file__)} \n Config Path-> {arguments.infile[0].name}", subject=f"INFO - SUCCESS | {config['environment']} | {config.get('source','api')} Ingestion | Google Maps API | {config['schema_name']}.{config['main_table']} {config['redshift_profile']}",log_path=log_path,logger=logger,add_on_email_stake_holders=config.get("add_on_email_stake_holders", None))
    except Exception as e:
        logger.error(f"Exception occured {e}")
        log_file_size= os.path.getsize(log_path)
        file_size=log_file_size / (1024 * 1024)
        if file_size < 1:send_email_notification(message=f"Script Path-> {os.path.abspath(__file__)} \n Config Path-> {arguments.infile[0].name} \n Exception -> {e} occured \n {traceback.format_exc()}", subject=f"FATAL | {config['environment']} | {config.get('source','api')} Ingestion | Google Maps API | {config['schema_name']}.{config['main_table']} {config['redshift_profile']}",log_path=log_path,logger=logger)
        else:send_email_notification(message=f"Script Path-> {os.path.abspath(__file__)} \n Config Path-> {arguments.infile[0].name} \n Log Path-> {log_path} \n Log File couldn't be uploaded due to size limit refer above log path for details \n Exception -> {e} occured \n {traceback.format_exc()}", subject=f"FATAL | {config['environment']} | {config.get('source','api')} Ingestion | Google Maps API | {config['schema_name']}.{config['main_table']} {config['redshift_profile']}",logger=logger)
