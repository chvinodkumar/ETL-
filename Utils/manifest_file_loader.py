
import boto3.session
from boxsdk import JWTAuth, Client
import argparse
import json
import os
import boto3
from datetime import datetime
import sys
import pandas as pd


class pattern_files_loader:

    def __init__(self,logger,audit_table,audit_schema,conn_engine,source_name,file_pattern,pattern_load_type,box_config_path=None,box_folderId=None,sftp_path=None,s3_profile=None,bucket=None,bucket_prefix=None):
        self.logger=logger
        self.source_name = source_name
        self.audit_table = audit_table
        self.audit_schmea = audit_schema
        self.file_pattern = file_pattern
        self.pattern_load_type = pattern_load_type
        self.id = box_folderId
        self.sftp_path = sftp_path
        self.s3_profile = s3_profile
        self.s3_bucket = bucket
        self.s3_bucket_prefix = bucket_prefix
        self.box_config_path = box_config_path
        self.engine = conn_engine
        #self.engine = get_connection(db_config, conn_profile, logger)
        self.main()

    def main(self):
       """
       A method is used to call the sub methods based on source system

       Parameters : None
       Return : None
       """
       try:

        if self.source_name.lower() == 'box':
                self.get_box_patternFiles()
        elif self.source_name.lower() == 'sftp':
                self.get_sftp_patternfiles()
        elif self.source_name.lower() == 's3':
                self.get_s3_patternfiles()
        else:
                raise ValueError("excepting source names as box, sfte or s3")
       except Exception as e:
           self.logger.info(f"Exception occured while execting main method in pattern_files_loader : {e}")
           raise

    def get_box_acces(self,JWT_file_path):
        """
        A method is used to enable the box authentication

        Parameters:
        JWT_file_path(str): Box auth config file path
        Return:
        Client (object): Client object after sucessfull authentication

        """
        auth = JWTAuth.from_settings_file(JWT_file_path)
        client = Client(auth)
        self.logger.info("Box authentication has been established successfully")
        return client

    def load_details_to_database(self,items):
        """
        A method is used to handle loadtype based on no of file counts and data loads into database

        Parameters:
           items(list) : list of pattern matched file names
        Returns: None
        """
        try:
            file_count = 1
            table_details= {}
            table_details["filename"]=[]
            table_details["load_type"]=[]
            for item in items:
                if file_count == 1 and self.pattern_load_type.lower() == 'truncate_load':
                    table_details["filename"].append(item)
                    table_details["load_type"].append("truncate_and_load")
                elif file_count > 1 and self.pattern_load_type.lower() == 'truncate_load':
                    table_details["filename"].append(item)
                    table_details["load_type"].append("fullload")
                elif self.pattern_load_type.lower() == 'incremental':
                    table_details["filename"].append(item)
                    table_details["load_type"].append("incremental")
                else:
                    self.logger.info(f"{item} file is not matched with pattern, file has been skipped")
                file_count += 1
            print(table_details)
            df = pd.DataFrame(table_details)
            df["load_timestamp"]=datetime.now()
            df.to_sql(self.audit_table, con=self.engine, schema=self.audit_schmea, if_exists='append', index=False, dtype=None)
            self.logger.info("Pattern matched file names inserted into audit table")
        except Exception as e:
            self.logger.info("Error occured while details loading to database due to error : {e}")
            raise

    def get_box_patternFiles(self):
        """
        A method is used to connect with box API and lists the files in manifest file

        Parameter:None
        Return None
        """
        try:
            client = self.get_box_acces(self.box_config_path)
            self.logger.info(f"pattern files searcing process has been started from box source system")
            if self.file_pattern is None:
                self.logger.info(f"file_pattern cannot be None")
                raise ValueError("file_pattern cannot be None")
            self.logger.info(f"Searching using file pattern is {self.file_pattern}")
            patterns=self.file_pattern.split('*')
            if len(patterns)==2:
                list_of_files = [x.name for x in client.folder(self.id).get_items() if x.name[0:len(patterns[0])]==patterns[0] and x.name[-len(patterns[1]):]==patterns[1]]
                self.load_details_to_database(list_of_files)
            else:
                raise ValueError("file_pattern must contain exactly one '*' character to define prefix and suffix")
        except Exception as e:
            self.logger.info(f"Exception occured while trying to get files from box : {e}")
            raise

    def get_sftp_patternfiles(self):
        """
        A method is used to fetch files from sftp path

        Parameters: None
        Returns : None
        """
        try:
            self.logger.info(f"pattern files searcing process has been started from sftp source system")
            if self.file_pattern is None:
                self.logger.info(f"file_pattern cannot be None")
                raise ValueError("file_pattern cannot be None")
            patterns=self.file_pattern.split('*')
            print(self.sftp_path)
            if len(patterns)==2:
                list_of_sftp_files = [x for x in os.listdir(self.sftp_path) if x[0:len(patterns[0])]==patterns[0] and x[-len(patterns[1]):]==patterns[1]]
                print(list_of_sftp_files)
                self.load_details_to_database(list_of_sftp_files)
            else:
                raise ValueError("file_pattern must contain exactly one '*' character to define prefix and suffix")
        except Exception as e:
            self.logger.info(f"Exception occured while trying to get files from box : {e}")
            raise
    def get_s3_patternfiles(self):
        """
        A method is used to get the pattern matched files from s3 bucket

        Parameters : None
        Returns : None

        """
        try:
            self.logger.info(f"pattern files searcing process has been started from S3 source system")
            if self.file_pattern is None:
                self.logger.info(f"file_pattern cannot be None")
                raise ValueError("file_pattern cannot be None")
            patterns=self.file_pattern.split('*')
            if len(patterns)==2:
                session = boto3.Session(profile_name=self.s3_profile)
                s3 = session.client('s3')
                print(self.s3_bucket,self.s3_bucket_prefix)
                response = s3.list_objects_v2(Bucket=self.s3_bucket, Prefix=self.s3_bucket_prefix)
                s3_files = [os.path.basename(obj['Key']) for obj in response['Contents']]
                print(s3_files)
                list_s3_files=[]
                for file in s3_files:
                    if file[0:len(patterns[0])]==patterns[0] and file[-len(patterns[1]):]==patterns[1]:
                        list_s3_files.append(file)
                self.load_details_to_database(list_s3_files)
                print(list_s3_files)
            else:
                raise ValueError("file_pattern must contain exactly one '*' character to define prefix and suffix")
        except Exception as e:
            self.logger.info(f"Exception occuered while fetching file names from s3 bucket error : {e}")
            raise
