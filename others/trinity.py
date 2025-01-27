from datetime import date, datetime
import fnmatch
import glob
from io import BytesIO
import logging
import sys
import tarfile
from boxsdk import Client, JWTAuth
import pandas as pd
import time
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, Boolean, func, Numeric
from decimal import Decimal, InvalidOperation
import json
import os
import configparser
from sqlalchemy import create_engine
import csv
import shutil
import os
import logging.config
import boto3

def send_email_notification(message, subject, config):
    try:
        os.system(f"""echo "{message}" | mailx -s "{subject} | {config['env']}"  -a "{config['log_file_location']}" "{config['email_stake_holders']}" """)
    except Exception as err:
        print(f"failed to send notification")

def read_config_file(filepath, connection):
    config = configparser.ConfigParser()
    config.read(filepath)
    db_name = config[connection]['dbname']
    user = config[connection]['user']
    password = config[connection]['password']
    host = config[connection]['host']
    port = int(config[connection]['port'])
    return db_name, user, password, host, port

def copy_to_redshift(config_info, schema_name, table_name, s3_file_prefix):
    try:
        DBNAME, USER, PASS, HOST, PORT = read_config_file(config_info['connections_filepath'], config_info['profile'])
        redshift_innvo_copy_cmd = f"""PGPASSWORD='{PASS}' psql -h {HOST} -p {PORT} -U {USER} -d {DBNAME} -t -c "TRUNCATE TABLE {schema_name}.{table_name}; COPY {schema_name}.{table_name} FROM 's3://{config_info["s3_bucket_name"]}/{s3_file_prefix}' CREDENTIALS 'aws_access_key_id={config_info["aws_access_key_id"]};aws_secret_access_key={config_info["aws_secret_access_key"]}' DELIMITER ',' FORMAT AS csv IGNOREHEADER 1" """
        print(redshift_innvo_copy_cmd)
        os.system(redshift_innvo_copy_cmd)
    except Exception as err:
        print(f"failed to send notification", err)

def setup_logger(file_name, log_config=None):
    default_log_config = {
        "version": 1,
        "formatters": {
            "mirroring": {
                "format": "%(asctime)s - %(levelname)s - %(message)s"
            }
        },
        "handlers": {
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "formatter": "mirroring",
                "filename": file_name,
                "mode": "a",
                "level": "INFO"
            }
        },
        "root": {
            "handlers": ['file'],
            "level": "INFO"
        }

    }
    if log_config:
        default_log_config.update(log_config)
    logging.config.dictConfig(default_log_config)
    logging.captureWarnings(True)
    logging.getLogger('boxsdk').setLevel(logging.ERROR)
    logging.getLogger('urllib3').setLevel(logging.ERROR)
    logging.getLogger('requests').setLevel(logging.ERROR)
    logging.getLogger('chardet').setLevel(logging.ERROR)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)
    logger = logging.getLogger(file_name)

    def handle_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        logger.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))

    sys.excepthook = handle_exception
    return logger

# Box class implementation
class Box:
    def __init__(self, logger:logging,  config: dict) -> None:
        self.config = config
        self.logger = logger
        self.client = self.get_access_to_box(os.getenv('BOX_CONFIG_PATH') or config['box_config_path'])
        self.folder_items = list(self.client.folder(self.config['folder_id']).get_items()) if self.client else []
        self.cur_date = datetime.now().strftime('%Y_%m_%d:%H_%M')

    def get_access_to_box(self, JWT_file_path: str) -> Client:
        try:
            auth = JWTAuth.from_settings_file(JWT_file_path)
            client = Client(auth)
            return client
        except Exception as err:
            self.logger.error(f"Error accessing Box: {err}")
            return None

    def filter_files_by_mask(self, file_mask: str) -> list:
        if not file_mask:
            return self.folder_items
        return [item for item in self.folder_items if fnmatch.fnmatch(item.name, file_mask)
                