#### Importing Necessary Packages ####
import logging
import logging.config
import os

def setup_logger(file_name, log_config=None):
    """
    A method to setup logger object

    Parameters:
    filename (str)   : Log file name
    log_config (str) : Log config for the given log file name

    Returns:
    logger (object) : engine object is returned which stores the connection to DB
    """
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
                "mode": "w",
                "level": "DEBUG"
            }
        },
        "root": {
            "handlers": ['file'],
            "level": "DEBUG"
        }
    }
    if log_config:
        default_log_config.update(log_config)
    logging.config.dictConfig(default_log_config)
    logging.captureWarnings(True)
    return logging.getLogger(file_name)

def send_email_notification(subject, message,logger, log_path='', email_stake_holders="Health_ODP_Ingestion_Alerts@ge.com,Health_ODP_Wissen_Ingestion_Ops@ge.com,cdoodpingestion-ams@ge.com,gehc_tcs_odp_ingestion_alerts@ge.com",add_on_email_stake_holders='' ):
    """
    A method to send alert on job failure to specified stake holders

    Parameters:
    subject (str)                    : Subject of the mail
    message (str)                    : Failure message to be included in the mail body
    logger (object)                  : Logger object to make log entries for the created log file
    log_path (object)                : Log file path which has to be attached to mail
    email_stake_holders (str)        : Pre defined stake holders which is default
    add_on_email_stake_holders (str) : Any add on mails to whom alert has to be sent should be provided as input to this argument

    Returns:None
    """
    try:
        email_recipients = email_stake_holders
        if add_on_email_stake_holders:
            email_recipients += f",{add_on_email_stake_holders}"
        logger.info(f"Executing send_email_notification method to send alert to {email_recipients}")
        if log_path:
            os.system(f"""echo "{message}" | mailx -s "{subject}" -a {log_path}  {email_recipients}""")
        else:
            os.system(f"""echo "{message}" | mailx -s "{subject}" {email_recipients}""")
        logger.info("Email alert has been sent to above mentioned email recipients")
    except Exception as err:
        logger.error(f"failed to send notification with error --> {err}")

########===============================================############

import configparser
import logging
import logging.config
from sqlalchemy import create_engine
import os

valid_load_types = ['TL', 'FL', "IL"]

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
                "mode": "w",
                "level": "DEBUG"
            }
        },
        "root": {
            "handlers": ['file'],
            "level": "DEBUG"
        }
    }
    if log_config:
        default_log_config.update(log_config)
    logging.config.dictConfig(default_log_config)
    logging.captureWarnings(True)
    return logging.getLogger(file_name)

def get_connection(filepath, profile):
    config = configparser.ConfigParser()
    config.read(filepath)
    db_name = config[profile]['dbname']
    user = config[profile]['user']
    password = config[profile]['password']
    host = config[profile]['host']
    port = int(config[profile]['port'])
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db_name}")
    return engine.connect().execution_options(autocommit=True)

def send_email_notification(subject, message, log_path='', email_stake_holders="Health_ODP_Ingestion_Alerts@ge.com, Health_ODP_Wissen_Ingestion_Ops@ge.com,cdoodpingestion-ams@ge.com,gehc_tcs_odp_ingestion_alerts@ge.com" ):
    try:
        if log_path:
            os.system(f"""echo "{message}" | mailx -s "{subject}" -a {log_path}  {email_stake_holders}""")
        else:
            os.system(f"""echo "{message}" | mailx -s "{subject}" {email_stake_holders}""")
    except Exception as err:
        print(f'failed to send notification: {err}')
