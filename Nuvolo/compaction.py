import pandas as pd
import json
import argparse
import time
from psycopg2.errors import DuplicateTable
from sqlalchemy.exc import ProgrammingError, InternalError
import boto3
import io
import configparser
from sqlalchemy import create_engine, types
from datetime import datetime, date, timedelta
from sqlalchemy.dialects import postgresql

redshift_keywords = list(postgresql.dialect().preparer.reserved_words)
dt = date.today()-timedelta(days=0)
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
schema_name = config_info['schema_name']

external_table=f'{table_name}_ext'

def read_config_file(filepath, connection):
    config = configparser.ConfigParser()
    config.read(filepath)
    db_name = config[connection]['dbname']
    user = config[connection]['user']
    password = config[connection]['password']
    host = config[connection]['host']
    port = int(config[connection]['port'])
    return db_name, user, password, host, port


DBNAME, USER, PASS, HOST, PORT = read_config_file(
    config_path, connectionprofile)
redshift = create_engine(
    f"postgresql://{USER}:{PASS}@{HOST}:{PORT}/{DBNAME}").connect().execution_options(autocommit=True)


def cast_column(clm, dtype):
    clm_dtype_map = {
        'datetime64': f"case when {clm} = 'null' then null when {clm} ='' then null else {clm}::timestamp end as {clm}",
        'int64': f"case when {clm} = 'null' then null when {clm} =' ' then null else replace({clm},',','')::int end as {clm}",
        'boolean': f"case when {clm} = 'null' then null when {clm} ='' then null else replace(replace( {clm} ,'false','0'),'true','1')::int::boolean end as {clm}",
        'object': f"case when {clm} = 'null' then null when {clm} ='' then null else {clm} end as {clm}"
        # 'character varying': clm
    }
    return clm_dtype_map[dtype]


def addCollation(dtype):
    cml_dtyp = {
        'object': 'character varying(65500) collate case_insensitive',
        'datetime64': 'timestamp',
        'boolean': 'boolean',
        'int64': 'int'
    }
    return cml_dtyp[dtype]


stg_df = pd.DataFrame([config_info['column_dtypes']])
stg_df = stg_df.T.reset_index().rename(columns={'index':'column_name', 0:'data_type'})
stg_df['column_name'] = stg_df['column_name'].apply(lambda x: f'"{x}"' if x in redshift_keywords else x)
stg_df['cast'] = stg_df.apply(lambda x: cast_column(x.column_name, x.data_type), axis=1)
casted_colmns = ', '.join(stg_df['cast'])
stg_df['collation'] = stg_df.apply(lambda x: x.column_name + ' ' + addCollation(x.data_type), axis=1)
collation_colmns = ', '.join(stg_df['collation'])
ref_col_df = pd.DataFrame(config_info['referance_columns'], columns=['referance_columns'])
referance_columns = ', '.join(ref_col_df['referance_columns'].apply(lambda x: x+'_id character varying(65500) collate case_insensitive'))

def get_full_load_id(col):
    return f"substring(json_extract_path_text({col}, 'link'), regexp_instr(json_extract_path_text({col}, 'link'),'/', 1, 7)+1, length(json_extract_path_text({col}, 'link'))) as {col}_id"


ref_col_df['full_sub_query'] = ref_col_df['referance_columns'].apply(lambda x: get_full_load_id(x))
full_sub_querys = ', '.join(ref_col_df['full_sub_query'])


def execute_query(query, success_msg='query executed successfully'):
    try:
        redshift.execution_options(isolation_level="AUTOCOMMIT").execute(query)
        print(f"###### {success_msg} #########")
    except ProgrammingError as e:
        if (isinstance(e.orig, DuplicateTable)):
            print("##########################################")
            print("######    Table already exist    #########")
            print("##########################################")
        elif "partition already exists." in str(e).lower():
            print('partition already exists')
        else:
            print(e)
    except InternalError as e:
        if "table already exists" in str(e).lower():
            print("External table already exists")
        elif "json parsing error" in str(e).lower():
            print("json parsing error")
        else:
            print(e)
    except Exception as err:
        print(err)

varchar_max_columns = ' Varchar(max), '.join(stg_df['column_name'].tolist())

create_external_table = f"create external table s3dataschema.{external_table}({varchar_max_columns} Varchar(max)) PARTITIONED BY (Timestamp varchar(5000)) stored as parquet location 's3://{config_info['AWSBUCKET']}/servicenow/{schema_name}/{table_name}/{year}/{month}/{datee}/'"

alter_ext_table=f"alter table s3dataschema.{external_table} add partition(Timestamp = '{year}-{month}-{datee}') location's3://{config_info['AWSBUCKET']}/servicenow/{schema_name}/{table_name}/{year}/{month}/{datee}/'"

if len(config_info['referance_columns'])>0:
    Create_stage_table = f"create table {schema_name}.{table_name}_stg({collation_colmns}, {referance_columns}, RowNumber bigint)"
    Create_main_table = f"create table {schema_name}.{table_name}({collation_colmns}, {referance_columns}, RowNumber bigint)"
    insert_from_ext_to_stg = f""" insert into {schema_name}.{table_name}_stg  SELECT {casted_colmns}, {full_sub_querys}, ROW_NUMBER()OVER(PARTITION BY {config_info['primary_key']}  ORDER BY sys_updated_on DESC) AS RowNumber FROM s3dataschema.{external_table}  """
else:
    Create_stage_table = f"create table {schema_name}.{table_name}_stg({collation_colmns}, RowNumber bigint)"
    Create_main_table = f"create table {schema_name}.{table_name}({collation_colmns}, RowNumber bigint)"
    insert_from_ext_to_stg = f""" insert into {schema_name}.{table_name}_stg  SELECT {casted_colmns}, ROW_NUMBER()OVER(PARTITION BY {config_info['primary_key']}  ORDER BY sys_updated_on DESC) AS RowNumber FROM s3dataschema.{external_table}  """

truncate_stg = f""" truncate table  {schema_name}.{table_name}_stg """
delete_dup_in_stg = f""" DELETE FROM {schema_name}.{table_name}_stg WHERE RowNumber > 1 """
delete_dup_in_main = f""" DELETE FROM {schema_name}.{table_name} USING {schema_name}.{table_name}_stg WHERE {schema_name}.{table_name}.{config_info['primary_key']}  = {schema_name}.{table_name}_stg.{config_info['primary_key']} """

insert_to_main = f""" INSERT INTO {schema_name}.{table_name} SELECT * FROM  {schema_name}.{table_name}_stg  """

def compaction():
    execute_query(create_external_table, "external table created successfully")
    execute_query(alter_ext_table, "partition created successfully")
    execute_query(Create_stage_table, "stage table created successfully")
    execute_query(Create_main_table, "main table created successfully")
    execute_query(truncate_stg, "truncate stage successfully")
    execute_query(insert_from_ext_to_stg, "ext to stg query successfully")
    execute_query(delete_dup_in_stg, "delete duplicats from stg query successfully")
    execute_query(delete_dup_in_main, "delete duplicats from main query successfully")
    execute_query(insert_to_main, "insert stg to main table query successfully")

compaction()
