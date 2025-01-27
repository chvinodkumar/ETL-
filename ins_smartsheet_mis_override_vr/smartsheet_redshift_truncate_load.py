
### Example Config File ###
"""{
    "source":"Source name for email alert",
    "environment":"Environment of the script location for email alert ",
    "sheet_id":"",
    "url":"",
    "auth_token":"Token to access api url",
    "ingestion_audit_field":"Name of the audit column that will be added in redshift table",
    "redshift_config":"Path where redshift credentials are stored",
    "redshift_profile":"Redshift profile",
    "log_file":"Log folder path no need to mention file name",
    "schema_name":"",
    "table_name":"",
    "parse_dates":"Column name which have to parsed/converted to datetime",
    "posting_agent":""
    "utils_path":"path where utils py file is placed to import functions in t",
    "data_length":"column data length for varchar type fields which has to be mentioned here"
}"""



########### Importing Packages ###############
import requests,json
import pandas as pd
from sqlalchemy import types
import argparse
import os,sys
from datetime import datetime
import urllib3


def auth(id,token):
    """
    A method to hit api and fetch response

    Parameters:
    id (str)    : Sheet id to fetch response from via API
    token (str) : Authentication Token used to hit API

    Returns:
    reponse (str) : Data types of the DataFrame and adding collation condition for varchar fields in the DataFrame
    """

    headers = { "Authorization" : f"Bearer {token}"}
    payload = {}
    url = f"{config['url']}{id}"
    response = requests.request("GET", url, headers=headers, data=payload)
    logger.info("Authentication Sucessfull")
    return response

def sqlcol(dfparam):
    """
    A static method to enable collation for varchar data fields

    Parameter:
    dfparam (DataFrame) : DataFrame to which collation has to be enabled

    Returns:
    dtypedict (dict) : Data types of the DataFrame and adding collation condition for varchar fields in the DataFrame
    """
    dtypedict = {}
    for i, j in zip(dfparam.columns, dfparam.dtypes):
        if "object" in str(j):
            dtypedict.update({i: types.VARCHAR(int(config["data_length"]), collation='case_insensitive')})
    logger.info("Collation Enabled")
    return dtypedict

def data(response):
    """
    A method to gather data from the response received by hitting with column names and data

    Parameter:
    response (str) : Response received by hitting API

    Returns:
    records (List) : List of values as column:Value
    """
    logger.info("Fetching records")
    data_cols = response.json()['columns']
    data_rows = response.json()['rows']
    columns={}
    for item in data_cols:
        columns.update({item['id']:item['title']})
    records=[]
    for row in data_rows:
        record={}
        for cell in row['cells']:
            if "value" in cell:
                record.update({columns[cell['columnId']]:cell['value']})
            else:
                record.update({columns[cell['columnId']]:''})
        records.append(record)
    return records

def dataframe_to_redshift(df,engine):
    """
    A method to gather data from the response received by hitting with column names and data

    Parameter:
    df (DataFrame)  : Response received by hitting API
    engine (object) : Redshift connection

    Returns:None
    """
    if "parse_dates" in config:
        for dates in config["parse_dates"].split(','):
            df[dates]=pd.to_datetime(df[dates])
    df.columns=pd.Series(df.columns).replace(' ','_',regex=True).str.lower()
    df["posting_agent"]=config["posting_agent"]
    con=engine.connect().execution_options(autocommit=True)
    try:
        con.execute(f"TRUNCATE TABLE {config['schema_name']}.{config['table_name']}")
        logger.info(f"{config['schema_name']}.{config['table_name']} has been truncated")
        logger.info("Loading Data to Redshift")
        df.to_sql(config["table_name"],  con=engine, schema=config["schema_name"], if_exists='append', method="multi", chunksize=1500, index=False, dtype=sqlcol(df))
        logger.info(f"Data loaded to {config['schema_name']}.{config['table_name']}")
        logger.info("Ingestion Completed")
    except Exception as e:
        if 'does not exist' in str(e):
            logger.info(f"{config['schema_name']}.{config['table_name']} does not exist")
            df.to_sql(config["table_name"],  con=engine, schema=config["schema_name"], if_exists='replace', method="multi", chunksize=1500, index=False, dtype=sqlcol(df))
            logger.info(f"{config['schema_name']}.{config['table_name']} Created")
            logger.info("Ingestion Completed")

def main():
    """
    A method to call other function accordingly to execute ingestion steps

    Parameter:None

    Returns:None
    """
    main_df=pd.DataFrame()
    for sheet in config["sheet_id"].split(','):
        api_response=auth(sheet,config["auth_token"])
        df = pd.DataFrame(data(api_response))
        df["data_orgin"]=sheet
        df[config["ingestion_audit_field"]]=datetime.today()
        logger.info("Data Fetched")
        main_df=pd.concat([main_df,df])
    dataframe_to_redshift(main_df,engine)

if __name__=="__main__":
    year, month, day = datetime.today().strftime("%Y"), datetime.today().strftime("%m"), datetime.today().strftime("%d")
    parser = argparse.ArgumentParser()
    parser.add_argument('--infile', nargs=1, help="JSON file to be processed", type=argparse.FileType('r'))
    arguments = parser.parse_args()
    config = json.load(arguments.infile[0])
    sys.path.append(config['utils_path'])
    from utils import setup_logger, send_email_notification,get_connection
    engine = get_connection(config["redshift_config"], config["redshift_profile"])
    log_filename = str(arguments.infile[0].name).split('/')[-1].replace('json', 'log')
    log_path=os.path.join(config["log_file"],log_filename)
    logger = setup_logger(log_path)

    try:
        logger.info("Ingestion Started")
        urllib3.disable_warnings()
        logger.info("Redshift connection established")
        sys.exit(main())
    except Exception as e:
        logger.error(f"Exception occured-> {e}")
        send_email_notification(message=f"Exception -> {e} occured at {os.path.abspath(__file__)} ,config path - {arguments.infile[0].name}", subject=f"{config['source']} Ingestion Failure for {config['schema_name']}.{config['table_name']}| {config['environment']}",log_path=log_path)

'''
{
    "source":"Smartsheet",
    "environment":"production",
    "sheet_id":"1860472375928708",
    "url":"https://api.smartsheet.com/2.0/sheets/",
    "auth_token":"2GWV1KIIxRjwDfGC2T413sg1YtMo4KKOtzyeU",
    "ingestion_audit_field":"ingestion_timestamp",
    "redshift_config":"/root/.aws/redshift_connection.ini",
    "redshift_profile":"Connections_PROD",
    "log_file":"/data-ingestion/custom_scripts/hdl_isc_ins_etl_stage/isc/logs/",
    "schema_name":"hdl_isc_ins_etl_stage",
    "table_name":"ins_smartsheet_mis_override_vr",
    "posting_agent":"smartsheet_isc",
    "utils_path":"/data-ingestion/custom_scripts/global/",
    "data_length":"4000"
}
'''