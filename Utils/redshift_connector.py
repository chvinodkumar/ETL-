#### Importing Necessary Packages ####
from sqlalchemy import create_engine
import configparser,os

def get_connection(filepath, profile,logger):
    """
    A method to establish DB connection

    Parameters:
    filepath (str)  : Path of the redshift configuration
    profile (str)   : Profile name of the redshift configuration
    logger (object) : Logger object to create log entries

    Returns:
    engine (object) : engine object is returned which stores the connection to DB
    """
    logger.info(f"Establishing connection to DB using get_connection method from {os.path.abspath(__file__)}")
    try:
        config = configparser.ConfigParser()
        config.read(filepath)
        db_name = config[profile]['dbname']
        user = config[profile]['user']
        password = config[profile]['password']
        host = config[profile]['host']
        port = int(config[profile]['port'])
        engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db_name}")
        logger.info(f"Connection established to redshift - '{profile}'")
        return engine.connect().execution_options(autocommit=True)
    except Exception as e:
        logger.error(f"Failed to establish connection to DB using get_connection method from {os.path.abspath(__file__)}")
