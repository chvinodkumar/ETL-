### Python script to handle soft deletes from log table
#######################################################
#Name: Naveen Marella #########soft_deletes.py
#Date: 02-09-2024
#Version : 1.0
#Version Comments: Initial Version
#Objective: Soft deletes module to handle soft deletes on main table using log based tables
#userstory:
#######################################################

from datetime import datetime
import time
import configparser
import json
import argparse
import sqlalchemy as sa
import os,sys

## Config file details
"""
- redshift_config_path:  Path to the Redshift connection details in the .aws file.
- redshift_connection_profile:  Connection profile specified in the connection details file.
- main_table_primary_key:  Primary key of the main table to compare with the log table's primary key.
- log_table_primary_key:  Primary key of the log table.
- main_table_schema:  Schema name of the main table.
- main_table:  Name of the main table that will handle soft deletes.
- log_table_schema:  Schema name of the log table.
- log_table_name:  Name of the log table that references the main table to handle soft deletes.


"""
class soft_deletes_wrapper:

     def __init__(self,config,logger) -> None:
           self.logger = logger
           self.config = config

     def data_updater(self):
        """
        A method that is calling Database function from redshift_loader module to perform soft_deletes

        Parameters : None
        Return     : None

        """

        if "main_table" and "log_table" in self.config:
                Database(load_type=self.config["load_type"],logger=logger,config=self.config["redshift_config"],profile=self.config["redshift_profile"],data=None,schema=self.config["schema_name"],main_table_name=self.config["main_table"],primary_key=self.config["main_table_primary_key"],log_table_primary_key=config["log_table_primary_key"],log_table = config["log_table"])
                print(f"Data base class has been executed sucessfully and soft_deletes are updated for {self.config['schema_name']}.{self.config['main_table']}")

        else:
                self.logger.info("No Data to ingest")



if __name__=="__main__":
        try:
            date = datetime.now()
            start_time = time.time()
            print(f"Script execting date :{date}")
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
            logger.info("Soft deletes handling process started")
            print("Soft deletes handling process started")
            data_ma = soft_deletes_wrapper(config, logger)
            data_ma.data_updater()
            end_time=time.time()
            script_execution_time = end_time - start_time
            print(f"Total script execution time : {script_execution_time}")


        except Exception as e:
            print(f"Exception occured due to : {e}")
            logger.info(f"Exception occured due to : {e}")
            send_email_notification(message=f"Exception -> {e} occurred at {os.path.abspath(__file__)}, config path - {arguments.infile[0].name}",subject=f"{config['source']} Ingestion Failure for {config['schema_name']}.{config['main_table']}| {config['environment']}",log_path=log_file,logger=logger,add_on_email_stake_holders=config.get("add_on_email_stake_holders", None))
