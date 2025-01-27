from datetime import datetime, timezone, timedelta
import glob
import os
from boxsdk import JWTAuth, Client
import json, csv, sys
import re,io
import pandas as pd
import requests,json
from io import BytesIO
import logging
import argparse
import configparser
from sqlalchemy import create_engine

#parsing the json path
parser = argparse.ArgumentParser()
parser.add_argument('--infile', nargs=1, help="JSON file to be processed", type=argparse.FileType('r'))
arguments = parser.parse_args()
config_info = json.load(arguments.infile[0])

##### Enabling the logging  ######
with open(f'{config_info["log_file_location"]}', 'a', newline='') as csvfl:
    write_csv = csv.writer(csvfl, delimiter='|')
    write_csv.writerow(['\t TimeStamp \t', 'Severity', '\t Message \t'])

    logging.basicConfig(filename = config_info["log_file_location"], format = ('%(asctime)s | %(levelname)s | %(message)s'))
    logger = logging.getLogger(config_info["log_file_location"])
    logger.setLevel(logging.INFO)

#Function to parse configurations
def read_config_file(connection):
    config = configparser.ConfigParser()
    config.read(config_info['config_path'])
    db_name = config[connection]['dbname']
    user = config[connection]['user']
    password = config[connection]['password']
    host = config[connection]['host']
    port = int(config[connection]['port'])
    return db_name, user, password, host, port

DBNAME, USER, PASS, HOST, PORT = read_config_file(config_info['connection_profile'])
engine = create_engine(f"postgresql://{USER}:{PASS}@{HOST}:{PORT}/{DBNAME}", executemany_mode='batch', pool_size=5, max_overflow=8).connect().execution_options(autocommit=True)

cur_date=datetime.now().strftime('%Y_%m_%d:%H_%M')
update_time =datetime.now()

try :
    #################getting access from the json file######################
    def getAccessToBox(JWT_file_path):
        auth = JWTAuth.from_settings_file(JWT_file_path)
        return Client(auth)

    ################get files to the local #########################
    def get_data_load(client,folder_id):
        filesLoaded=list()
        for item in client.folder(folder_id).get_items():
            try :
                if item.type == "folder":
                    print(f"{item} is a folder")
                elif item.type == "file" :
                    filename=client.file(item.id).get().name
                    df = pd.DataFrame() # Intializing empty dataframe
                    if filename.__contains__(".xlsx") and filename.startswith(tuple(config_info['filename'])):
                        data = client.file(item.id).content()
                        df = pd.read_excel(BytesIO(data), engine='openpyxl', header=0, names=config_info['columnnames'], dtype=object)
                    elif filename.__contains__(".csv") and filename.startswith(tuple(config_info['filename'])):
                        data = client.file(item.id).content()
                        df = pd.read_csv(BytesIO(data), header=0, names=config_info['columnnames'], dtype=object)  #load data into dataframe
                    if len(df)> 0 :
                        logger.info(f"current processing file {filename}")
                        load_status = load_to_redshift(df, filename, engine, config_info['tablename'], config_info['schemaname'])
                        if load_status == 'success':
                             move_to_archive(item) # move to archive on success load
                    filesLoaded.append(item)
            except Exception as e:
                logger.info(f'some error occured while executing : {e}')
                os.system(f'echo "{config_info["Mail_notification"]["Ingestion_failure"]} . Error Message : {str(e)[:500]} and Please refer to logfile : {config_info["log_file_location"]}" | mailx -s "{config_info["Mail_notification"]["Subject"]}" {config_info["Mail_notification"]["dl"]}')
        return filesLoaded

    ###### load to database #######
    def load_to_redshift(df,filename, conn, tablename, schemaname):
        load_status = ''
        try :
            df["data_origin"]=filename
            df["posting_agent"]='python'
            df["load_dtm"] = update_time
            df = df.where(pd.notnull(df), None)
            tablename=tablename.lower()
            exist_query=f"""SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = '{schemaname}' AND tablename = '{tablename}')"""
            STATUS = engine.execute(exist_query).fetchall()
            if(STATUS[0][0] == True or STATUS[0][0] == 't'):
                truncate_query=f"""truncate table {schemaname}.{tablename}"""
                engine.execute(truncate_query)
                logger.info(f"the data in the {schemaname}.{tablename} is truncated ")
            df.to_sql(tablename, con=conn, schema=schemaname, if_exists='append', index=False, chunksize=5000, method='multi')
            load_status = 'success'
            logger.info(f"Ingested table {schemaname}.{tablename} on {cur_date} with shape {df.shape} successfully")
        except Exception as e:
            logger.info(f'some error occured while executing : {e}')
            os.system(f'echo "{config_info["Mail_notification"]["Ingestion_failure"]} . Error Message : {str(e)[:500]} and Please refer to logfile : {config_info["log_file_location"]}" | mailx -s "{config_info["Mail_notification"]["Subject"]}" {config_info["Mail_notification"]["dl"]}')
        return load_status
    #############moving files to archieve location in box################
    def move_to_archive(item):
        file_to_move = client.file(item.id).rename(f"_{cur_date}.".join(item.name.split('.')))
        destination_folder_id = config_info['archival_folder_id']
        destination_folder = client.folder(destination_folder_id)
        moved_file = file_to_move.move(parent_folder=destination_folder)
        logger.info(f"file{file_to_move} moved to archieved loaction")
except Exception as e :
    logger.info(f'some error occured while executing : {e}')
    os.system(f'echo "{config_info["Mail_notification"]["Ingestion_failure"]} . Error Message : {str(e)[:500]} and Please refer to logfile : {config_info["log_file_location"]}" | mailx -s "{config_info["Mail_notification"]["Subject"]}" {config_info["Mail_notification"]["dl"]}')

 ######### Main method ##########
if __name__ == "__main__":
    client = getAccessToBox(config_info['box_conf'])
    get_data_load(client, config_info['box_folder_id'])


'''
{
    "box_conf" : "/root/.aws/conf.json",
    "config_path" : "/root/.aws/redshift_connection.ini",
    "connection_profile":"Connections_PROD",
    "box_folder_id" : "269795250462",
    "filename" : ["GEHC_HS_WithDutyRates_"],
    "schemaname" : "isc_box_file_fr",
    "columnnames" : ["co","hts","descr","type","dutyrate"],
    "tablename" : "hrz_duty_rates",
        "archival_folder_id" : "269795059103",
    "log_file_location" :"/data-ingestion/custom_scripts/isc/log/box_gehc_hs_withdutyrates.log",
        "Mail_notification" :
    {
    "dl" : "cdoodpingestion-ams@ge.com",
    "Subject" : "isc_box_ingestion : Failure ",
    "Ingestion_failure" : "Exception while loading data to the database ."
    }
}
'''
'''===================

#######################################################
#Name: Kola Devi Revanth
#Date: 05-08-2024
#Version : 1.0
#Version Comments: Initial Version
#Objective: Box Module to handle CSV and Excel single and multiple files to one table only if column structure is same
#userstory:
########################################################

#### Importing Necessary Packages ####
import sys,os
from datetime import datetime
from io import BytesIO
from boxsdk import JWTAuth, Client
import pandas as pd
import argparse
import json

### Sample Config File ###
{
    "box_id":"Box id can be mutiple or single if multiple separate them by commas eg- 1,2,3",
    "source":"source name",
    "environment":"dev / Prod",
    "box_config_path":"Box config path",
    "file_names":"File name that has to be picked for ingestion if multiple files can be mentioned seperated by commas eg - test.csv,test2.csv",
    "ingestion_audit_field":"Name of the field that will be added in table as ingestion audit",
    "log_file_path":"log path where log file has to be created",
    "schema_name":"Target Schema name",
    "main_table":"Target Table name",
    "file_type":"csv / excel / text has to be mentioned",
    "stage_table":"Target Stage table name keep it empty if not required but it has be present in config",
    "data_origin":"Name of the column which will store source box file id as value in Table",
    "posting_agent":"Name of the column which will store source box file name as value in Table",
    "required_excel_features":"any extra attributes need to read_excel can be added here as a dict eg - {'method':'value'}",
    "required_csv_features":"any extra attributes need to read_csv can be added here as a dict eg - {'method':'value'}",
    "load_type":"incremental/truncate_and_load/fullload",
    "redshift_config":"Path to redshift configuration",
    "add_on_email_stake_holders":"Only to be added when extra email stake holders had to be included or else neglect this key value",
    "utils_path":"Path to the utility py file which has logging and alert feature on failure compulsory input in config",
    "redshift_profile":"Redshift profile ",
    "archive_folder":"This has to only be added in config is archival is required or else this field can be removed completely",
    "primary_key":"Only need for incremental should be kept as empty if not required config expects this parameter"
}


class DataFetcher:
    # A class to fetch files from Box API and ingest them to S3 or Redshift depending on the inputs provided
    def __init__(self, config, logger) -> None:
        """
        The Constructor for DataFetcher class.

        Parameters:
        config (dict): Configuration dictionary
        logger (Logger): Logger object
        engine (object): Redshift connection engine
        """
        self.config = config
        self.logger = logger

    def box_access(self, JWT_file_path):
        """
        A method to enable authentication to box api

        Parameters:
        JWT_file_path (str) : File path of the box config file sotred

        Returns:
        client (object) : Client object after sucessfull authentication
        """
        auth = JWTAuth.from_settings_file(JWT_file_path)
        client = Client(auth)
        self.logger.info("Box authentication has been established successfully")
        return client

    def get_excel_box(self, client, folder_id):
        """
        A method to read excel file from box location and return the data frame it can handle multiple sheets or multiple files given that column structure is similar across all files to single table

        Parameters:
        client (object) : Client object initiated for box authentication
        folder_id (str) : Box folder id

        Returns:
        df (DataFrame) : DataFrame which holds data fetched from box location
        items (list)   : Box file id or ids if multiple files are provided
        """
        df = pd.DataFrame()
        items=[]
        for item in client.folder(folder_id).get_items():
            if item.type == "file" and item.name in self.config["file_names"].split(','):
                box_file_name = client.file(item.id).get().name
                self.logger.info(f"{box_file_name} has been downloaded from Box")
                buffer = BytesIO()
                client.file(item.id).download_to(buffer)
                buffer.seek(0)
                if self.config["required_excel_features"]:
                    if self.config["sheet_names"]:
                        for sheet in self.config["sheet_names"].split(','):
                            main_df = pd.read_excel(io=BytesIO(buffer.read()),sheet_name=sheet,**self.config["required_excel_features"])
                    else:main_df=pd.read_excel(io=BytesIO(buffer.read()),**self.config["required_excel_features"])
                else:main_df=pd.read_excel(io=BytesIO(buffer.read()))
                main_df["data_origin"]=item.id
                main_df["posting_agent"]=item.name
                df = pd.concat([df, main_df])
                items.append(item.id)
        return df,items

    def get_csv_box(self, client, folder_id):
        """
        A method to read csv file from box location and return the data frame it can handle multiple files given that column structure is similar across all files to single table

        Parameters:
        client (object) : Client object initiated for box authentication
        folder_id (str) : Box folder id

        Returns:
        df (DataFrame) : DataFrame which holds data fetched from box location
        items (list)   : Box file id or ids if multiple files are provided
        """
        df = pd.DataFrame()
        items=[]
        for item in client.folder(folder_id).get_items():
            if item.type == "file" and item.name in self.config["file_names"].split(','):
                box_file_name = client.file(item.id).get().name
                self.logger.info(f"{box_file_name} has been downloaded from Box")
                buffer = BytesIO()
                client.file(item.id).download_to(buffer)
                buffer.seek(0)
                if "required_csv_features" in self.config:main_df = pd.read_csv(filepath_or_buffer=BytesIO(buffer.read()),**self.config["required_csv_features"])
                else:main_df = pd.read_csv(filepath_or_buffer=BytesIO(buffer.read()))
                main_df[self.config["data_origin"]]=item.id
                main_df[self.config["posting_agent"]]=item.name
                df=pd.concat([df,main_df])
                items.append(item.id)
        return df,items

    def move_to_archive(self, client, item_list):
        """
        A method archive folders from one box location to other it can handle multiple files to only one single box location

        Parameters:
        client (object)  : Box authentication object
        item_list (list) : list of box file id or in case multiple files ids that has to be archieved

        Returns: None
        """
        for item in item_list:
            file_to_move = client.file(item.id).rename(f"_{day}-{month}-{year}.".join(item.name.split('.')))
            destination_folder_id = self.config["archive_folder"]
            destination_folder = client.folder(destination_folder_id)
            file_to_move.move(parent_folder=destination_folder)
            self.logger.info(f"file {file_to_move} moved to archive location")

    def main(self):
        """
        A method to initiate Box ingestion based on the inputs provided in config

        Parameters: None

        Returns: None
        """
        try:
            client = self.box_access(self.config["box_config_path"])
            if self.config["file_type"].lower()=='csv' or self.config["file_type"].lower()=='text':
                df,items = self.get_csv_box(client, self.config["box_id"])
            elif self.config["file_type"].lower()=='excel':
                df,items = self.get_excel_box(client, self.config["box_id"])
            if not df.empty:
                if self.config["replace_space_in_column_name"].lower()=='y':
                    df.columns = pd.Series(df.columns).replace(' ', '_', regex=True).str.lower()
                else:
                    df.columns = pd.Series(df.columns).str.lower()
                df[self.config["ingestion_audit_field"]] = datetime.today()
                if "schema_name" and "main_table" in self.config:
                    Database(load_type=self.config["load_type"],logger=logger,config=self.config["redshift_config"],profile=self.config["redshift_profile"],data=df,schema=self.config["schema_name"],main_table_name=self.config["main_table"],stage_table_name=self.config["stage_table"],primary_key=self.config["primary_key"])
                if "archive_folder" in self.config:
                    self.move_to_archive(client,self.config["file_names"],items)
                self.logger.info("Ingestion Completed")
            else:self.logger.info("No data to ingest")
        except Exception as e:
            self.logger.error(f"Failed to execute main method in DataFetcher class , error --> {e}")
            raise

if __name__ == "__main__":
    start = datetime.today().strftime("%Y-%m-%d_%H:%M:%S")
    year, month, day = datetime.today().strftime("%Y"), datetime.today().strftime("%m"), datetime.today().strftime("%d")
    parser = argparse.ArgumentParser()
    parser.add_argument('--infile', nargs=1, help="JSON file to be processed", type=argparse.FileType('r'))
    arguments = parser.parse_args()
    config = json.load(arguments.infile[0])
    parent_path = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0,config["utils_path"])
    from utils import setup_logger, send_email_notification
    from redshift_loader import Database
    log_filename = str(arguments.infile[0].name).split('/')[-1].replace('json', 'log')
    log_file=os.path.join(config["log_file_path"], log_filename)
    logger = setup_logger(log_file)
    logger.info("Ingestion Started")
    try:
        data_fetcher = DataFetcher(config, logger)
        sys.exit(data_fetcher.main())
    except Exception as e:
        logger.error(f"Exception occurred: {e}")
        send_email_notification(message=f"Exception -> {e} occurred at {os.path.abspath(__file__)}, config path - {arguments.infile[0].name}",subject=f"{config['source']} Ingestion Failure for {config['schema_name']}.{config['main_table']}| {config['environment']}",log_path=log_file,logger=logger,add_on_email_stake_holders=config.get("add_on_email_stake_holders", None))
'''