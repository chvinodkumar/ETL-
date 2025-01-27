
import configparser
import sqlalchemy as sa
from sqlalchemy import types
import pandas as pd
import boto3
from datetime import date, datetime, timedelta
import io
import json
import time
import traceback
import logging
import csv
import sys
import os
import pyarrow.parquet as pq

start_time = time.time()
config_json = sys.argv[1]
with open(config_json) as f:
    config = json.load(f)

bucket_name = config["bucket_name"]
schema_name = config["schema_name"]
log_file1 = config["log_file1"]
aws_access_key_id = config['aws_access_key_id']
aws_secret_access_key = config['aws_secret_access_key']
working_directory = config['working_directory']
opr_dl = config['opr_dl']
exclude_tables_lst = config['exclude_tables_lst']
time_stamp_tbls = config['time_stamp_tbls']
tables_lst1 = config["tables_lst1"]
print("list1 tables : ", tables_lst1)
print("Exclude Tables: ", exclude_tables_lst)

with open(log_file1, 'w', newline='') as csv_file:
    write_csv = csv.writer(csv_file, delimiter='|')
    write_csv.writerow(['\t TimeStamp \t', 'Severity', '\t Message \t'])

logging.basicConfig(filename=log_file1,level=logging.INFO, format=('%(asctime)s | %(levelname)s | %(message)s'))
logger = logging.getLogger('tables')

def read_config_file(filepath, connection):
    config = configparser.ConfigParser()
    config.read(filepath)
    db_name = config[connection]['dbname']
    user = config[connection]['user']
    password = config[connection]['password']
    host = config[connection]['host']
    port = int(config[connection]['port'])
    return db_name, user, password, host, port

DBNAME, USER, PASS, HOST, PORT = read_config_file(config["config_path"], config["connection_profile"])
db_connection_string = f"postgresql://{USER}:{PASS}@{HOST}:{PORT}/{DBNAME}"

def sqlcol(d_type):
    dtypedict = {}
    for i, j in d_type.items():
        if "object"in str(j):
            dtypedict.update({i: types.VARCHAR(65535,collation='case_insensitive')})
        elif "float64" in str(j):
                dtypedict.update({i: types.DECIMAL()})
        elif "int64" in str(j):
                dtypedict.update({i: types.BIGINT()})
        elif "bool" in str(j):
                dtypedict.update({i: types.BOOLEAN()})
    return dtypedict

def process_parquet_files(lst_objects):
    print("Total Tables count", len(lst_objects))
    logger.info(f"list of objects received")
    failed_tables = []
    print("Start time :", datetime.today())
    for obj in lst_objects:
        start_time_tbl = time.time()
        obj_key = obj["Key"]
        file_name = obj_key.split('/')[1]
        cur_date = datetime.today()
        table_name = file_name.lower().replace("dbo.",'').replace(".parquet",'')
        if table_name in exclude_tables_lst:
            print("Exculde Table Name:", table_name)
            continue
        if table_name in tables_lst1:
            try:
                print("Table Name :", table_name)
                logger.info(f"\n Process started for {schema_name}.{table_name} table")
                response = s3.get_object(Bucket=bucket_name, Key=obj_key)
                parquet_data = response['Body'].read()
                if table_name in time_stamp_tbls:
                    df = pq.read_table(io.BytesIO(parquet_data)).to_pandas(safe=False)
                else:
                    df = pd.read_parquet(io.BytesIO(parquet_data))
                df["ingested_timestamp"]=cur_date
                with sa.create_engine(db_connection_string, executemany_mode='batch',
                                  pool_size=5, max_overflow=8).connect().execution_options(autocommit=True) as conn:
                    if sa.inspect(conn).has_table(table_name, schema=schema_name):
                        logger.info(f"\n Truncate and loading {schema_name}.{table_name} table")
                        conn.execute(f"TRUNCATE TABLE {schema_name}.{table_name}")
                        df.to_sql(table_name, con=conn, if_exists='append', chunksize=1000, index=False, schema=schema_name, method="multi",dtype= sqlcol(df))
                    else:
                        logger.info(f"\n Creating {schema_name}.{table_name} table")
                        df.to_sql(table_name, con=conn, if_exists='replace', chunksize=1000, index=False, schema=schema_name, method="multi",dtype= sqlcol(df))
                end_time_tbl = time.time()
                elapsed_time_tbl = (end_time_tbl - start_time_tbl) / 60
                logger.info(f'\n Total process duration for  {schema_name}.{table_name} table is ' + str(elapsed_time_tbl) + ' mins' + '\n')
            except Exception as err:
                print("Table ingestion failed")
                failed_tables.append(f'{table_name}')
                os.system(f'echo "This table is failed for this table {table_name} due to this error : {err}" | mailx -s "EHS Sensitive ODP Dev: HVR Ingestion: EHS Sensitive : Failure" {opr_dl}')
                logger.error(traceback.format_exc())
    print("end time:", datetime.today())
    end_dt = datetime.today()
    logger.info(f'endtime : {end_dt}')
    config['failed_tables'] = failed_tables
    with open(f'{working_directory}ingest_parquet.json', 'w') as f:
        f.write(json.dumps(config))
try:
    curt_date = date.today() - timedelta(days=0)
    prefix = curt_date.strftime("%Y-%m-%d")
    print(prefix)
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    lst_objects = s3.list_objects(Bucket=bucket_name, Prefix=prefix)["Contents"]
    total_files = len(lst_objects)
    os.system(f'echo "Total files available at EHS Sensitive Bucket :{total_files}." | mailx -s "EHS gensuite sensitive_schema ODP Dev: HVR Ingestion: EHS gensuite : Information" {opr_dl}')
    process_parquet_files(lst_objects)
    logger.info(f'Ingestion process started')
    end_time = time.time()
    elapsed_time = (end_time - start_time) / 60
    logger.info('\n Total process duration is ' + str(elapsed_time) + ' mins')

except Exception as err:
    logger.error(traceback.format_exc())
#==========================
'''
'''