import json
import argparse
import time
from psycopg2.errors import DuplicateTable
from sqlalchemy.exc import ProgrammingError
import boto3
import io
import configparser
from sqlalchemy import create_engine, types
import requests
from datetime import datetime, date
import pandas as pd
import json
import sys
import os

# overview

start_time = time.time()
dt = date.today()
year, month, datee = dt.strftime('%Y'), dt.strftime('%m'), dt.strftime('%d')

parser = argparse.ArgumentParser()
parser.add_argument('--infile', nargs=1, help="JSON file to be processed", type=argparse.FileType('r'))
arguments = parser.parse_args()


# Loading a JSON object returns a dict.
config_info = json.load(arguments.infile[0])

config_path = config_info['config_path']
connectionprofile = config_info['connectionprofile']
AWSPROFILE = config_info['AWSPROFILE']
AWSBUCKET = config_info['AWSBUCKET']

filepath = config_info['filepath']
table_name = config_info['table_name']
schema_name=config_info['schema_name']

us, pswd = config_info['credentials']
URl = f"{config_info['URL']}/{config_info['table_name']}?"
limit=config_info['limit']


print(config_info['table_name'])

def execute_cmd(cmd):
    try:
        os.system(cmd)
    except Exception as err:
        print(err)
        print(f'System Command execution failed')

def read_config_file(filepath, connection):
    """
    Read Dasbase config info

    Parameters
    ----------
    Filepath : str
        Config file path has to be passed
    Connection : str
        It takes the specific Datadase info.

    Returns
    -------
    array : ndarray or ExtensionArray
        DBNAME, USER, PASS, HOST, PORT for the specific connection

    Raises
    ------
    TypeError
        if incompatible Connection is provided, equivalent of same_kind
        casting
    """
    config = configparser.ConfigParser()
    config.read(filepath)
    db_name = config[connection]['dbname']
    user = config[connection]['user']
    password = config[connection]['password']
    host = config[connection]['host']
    port = int(config[connection]['port'])
    return db_name, user, password, host, port

# function call returns the datables info
DBNAME, USER, PASS, HOST, PORT = read_config_file(config_path, connectionprofile)
redshift = create_engine(f"postgresql://{USER}:{PASS}@{HOST}:{PORT}/{DBNAME}").connect().execution_options(autocommit=True)

def execute_query(query):
    """
    Execute the provided query

    Parameters
    ----------
    query : str
        SQL Query

    Returns
    -------
    None

    Raises
    ------
    TypeError
        if incompatible query is provided
    """
    try:
        redshift.execute(query)
    except Exception as err:
        # send an email to DL
        print(err)
        print("Query Failed")

def load_to_s3(BUCKET, temp_df, s3_path):
    """
    Read Dasbase config info

    Parameters
    ----------
    Filepath : str
        Config file path has to be passed
    Connection : str
        It takes the specific Datadase info.

    Returns
    -------
    None

    Raises
    ------
    TypeError
        if incompatible Connection is provide.
    """
    session = boto3.Session(profile_name=AWSPROFILE)
    client = session.client('s3')
    csv_buf = io.BytesIO()
    temp_df.to_parquet(csv_buf, index=False)
    client.put_object(Bucket=BUCKET, Body=csv_buf.getvalue(), Key=s3_path)
    print(temp_df.shape, ': Shape of this dataframe loaded to s3')

def make_api_request(url):
    """
    Make an api request

    Parameters
    ----------
    URL : str
        Takes an API URL

    Returns
    -------
    response: Response Object
        returns response status and headers
    result : ndarray or ExtensionArray
        returns api response data

    Raises
    ------
    TypeError
        if incompatible URL is provide.
    """
    try:
        res = requests.request("GET", url, auth=(us, pswd))
        result = res.json()['result']
        return res, result
    except Exception as err:
        execute_cmd(f"""echo "Api call failed for the {config_info["x_nuvo_eam_clinical_work_orders"]} table" | mailx -s "Ingestion Failed"  {config_info["email_stake_holders"]}""")
        print("catchup error")
        print(err[:-10000])

def df2S3(data, page):
    """
    load dataframe to s3 location

    Parameters
    ----------
    data : Array
        This takes the Array of object as data
    page :  Int
        it's a offset value

    Returns
    -------
    None

    Raises
    ------
    TypeError
        if incompatible URL is provide.
    """
    try:
        filename = f'{str(page)}_{table_name}_{date.today().strftime("%Y%m%d%H%M%S")}.parquet'
        df = pd.DataFrame(data).fillna('')
        df = df.apply(lambda x: x.apply(lambda y: json.dumps(y) if isinstance(y, dict) else y))
        df = df.astype(str)
        max_date_time = pd.to_datetime(df['sys_updated_on'], format='%Y-%m-%d %H:%M:%S').max().strftime('%Y-%m-%d %H:%M:%S').split(' ')
        print("max time --- ", max_date_time ,"-- at offset -- ", page)
        # df.to_parquet(filename)
        load_to_s3(AWSBUCKET, df, f'servicenow/{schema_name}/{table_name}/{year}/{month}/{datee}/{filename}')
        print("df loaded to s3")
        max_timestamp = " ".join(max_date_time)
        audit_insert_query=f"INSERT INTO {config_info['audit_schema_table']} VALUES (0, '{schema_name}', '{table_name}',  'nuvolo_df','sys_updated_on' ,'{max_timestamp}')"
        execute_query(audit_insert_query)
    except Exception as err:
        execute_cmd(f"""echo "Ingestion for the {config_info["x_nuvo_eam_clinical_work_orders"]} table failed at s3" | mailx -s "Ingestion Failed"  {config_info["email_stake_holders"]}""")
        print("Ingestion error")
        print(err)
        sys.exit()

try:
    #  get min date from mirror table for incremental
    maxdate, maxtime = redshift.execute(f"select max(max_timestamp) from {config_info['audit_schema_table']} where table_name='{config_info['table_name']}' and schema_name='{config_info['schema_name']}'").fetchall()[0][0].split(' ')
    incremntal=True
except Exception as err:
    #  get min date from config file for Full load
    maxdate, maxtime = config_info['min_datetime']
    incremntal=False

offset=0
count=0
remaining_records=True

# Pagination to fetch all the records for the specfic table
# If it fail's at any page it will tries 3 times after 3 sec
# If the job is set in incremntal mode then system query will have sys_updated_on greater then query
# Else only order by is added to sysparm_query to avoild the max exectuion transation failure
# If it the last next page and if it have zero records or X-Total-Count value is zero then it will wxit from the loop

while remaining_records:
    for tries in range(3):
        try:
            parms = f"&sysparm_display_value=true&sysparm_suppress_pagination_header=true&sysparm_limit={limit}&sysparm_offset={offset}"
            if incremntal:
                sysparm_query = f"&sysparm_query=sys_updated_on>=javascript:gs.dateGenerate('{maxdate}', '{maxtime}')%5E{config_info['sysparm_query']}"
            else:
                sysparm_query = f"&sysparm_query={config_info['sysparm_query']}"
            sysparm_fields="sysparm_fields="+"%2C".join(config_info['column_dtypes'].keys())
            url = URl + sysparm_fields + sysparm_query + parms
            res, data = make_api_request(url)
            if not (len(data)>0 and int(res.headers['X-Total-Count'])>0):
                remaining_records=False
                break
            print(f"{len(data)} records loaded remaining - {res.headers['X-Total-Count']}")
            df2S3(data, offset)
            offset+=limit
        except Exception as e:
            print(e)
            print(f'Exception created at try number {tries}')
            time.sleep(3)
            if tries == 2:
                print(f'Max tries reaches, moving to next iteration')
