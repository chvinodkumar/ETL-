
"""
{
"logfile" : "Log folder path with file name",
"config_path" : "redshift connection path",
"connection_profile": "redshift connection profile",
"profilename" : "profile_name_s3_bucket",
"BUCKETNAME" :  "s3_bucket_name",
"FOLDERNAME"  :  "s3_folder_name where files are present",
"TARGET_FOLDERNAME" : "s3_archive_folder_name to move files to",
"schemaname"     : "target schema name",
"Table_name" : "table name in target",
"truncate" : true/false (true to truncate ,false for append,
"cols" : ["list all column names in order"],
"chunks" : "Mention chunk size in integers to load",
"Mail_notification" :
    {
    "dl" : "mail_dl",
    "Subject" : "Subject_for_mail_alert",
    "s3_connection_failure" : "Exception while connecting to s3 client  ",
    "s3_list_error"    : "Exception while listing the keys in s3 bucket ",
    "s3_get_object_error"  : "Exception while getting the data from the key in s3 bucket ",
    "s3_move_object_error" : "Exception while archiving the key from s3 bucket",
    "Ingestion_failure" : "Exception while loading data to the database ."
    }
}
"""
#Starting the script #

## importing the required modules
import boto3,os,csv
import pandas as pd
import configparser,sys,json
from io import BytesIO
import sqlalchemy as sa
from datetime import datetime
from pathlib import Path
import logging

# function to establish connection object s3 client #
def s3_connect(profilename):
    try:
        session = boto3.Session(profile_name=profilename)
        s3_client = session.client('s3')
        return s3_client
    except Exception as e:
        logging.error(f'Error while establishing the s3 clinet : {e}')
        os.system(f'echo "{s3_connection_failure} . Error Message : {str(e)[:500]} and Please refer to logfile : {logfile}" | mailx -s "{Subject}" {dl}')

# function to list the files in s3 bucket #
def s3_list_objects(s3_client,BUCKETNAME,FOLDERNAME):
    try:
        response = s3_client.list_objects_v2(Bucket = BUCKETNAME,Prefix = FOLDERNAME,Delimiter ='/')
        if 'Contents' in response:
            files = [obj['Key'] for obj in response['Contents'] if not obj['Key'].endswith('/')]
            return files
        else:
            return []
    except Exception as e:
        logging.error(f'Error while listing the Object in the s3 bucket : {e}')
        os.system(f'echo "{s3_list_error} . Error Message : {str(e)[:500]} and Please refer to logfile : {logfile}" | mailx -s "{Subject}" {dl}')

# function to get the data of the key using get_object method #
def s3_get_object(s3_client,BUCKETNAME,filename):
    try:
        get_response = s3_client.get_object(Bucket = BUCKETNAME,Key = filename ,ResponseContentEncoding = 'utf-8')
        if get_response['ResponseMetadata']['HTTPStatusCode']==200:
            data =  get_response['Body']
            return data
    except Exception as e:
        logging.error(f'Error while geeting the data from the s3 object : {e}')
        os.system(f'echo "{s3_get_object_error} . Error Message : {str(e)[:500]} and Please refer to logfile : {logfile}" | mailx -s "{Subject}" {dl}')

# function to archive the files from one bucket to other #
def s3_move_object(s3_client,BUCKETNAME,FOLDERNAME,TARGET_FOLDERNAME,filename):
    try:
        filename = filename.split('/')[-1]
        Source_key = FOLDERNAME + filename   # Source Key
        Target_Key = TARGET_FOLDERNAME + filename   # Target Key
        copy_source = {'Bucket': f'{BUCKETNAME}','Key': f'{Source_key}'}
        s3_client.copy(copy_source,Bucket=BUCKETNAME,Key=Target_Key)    # copy file to s3 archival
        s3_client.delete_object(Bucket=BUCKETNAME,Key=Source_key)   # Delete file in s3 bucket after archival
        logging.info(f'(Moving {Source_key} to {Target_Key})')
    except Exception as e:
        logging.error(f'Error while moving the s3 object : {e}')
        os.system(f'echo "{s3_move_object_error} . Error Message : {str(e)[:500]} and Please refer to logfile path : {logfile}" | mailx -s "{Subject}" {dl}')

# Pick from the following
# ['Connections_PROD', 'Connections_INNOVATION', 'Connections_FINNPROD', 'Connections_FINPROD']

# function to establish get the configuration params
def read_config_file(filepath, connection):
    config = configparser.ConfigParser()
    config.read(filepath)
    db_name = config[connection]['dbname']
    user = config[connection]['user']
    password = config[connection]['password']
    host = config[connection]['host']
    port = int(config[connection]['port'])
    return db_name, user, password, host, port

#-------------------  reading the parameters from json file  ------------------- #
ARGV = sys.argv
json_path = ARGV[1]

with open(f'{json_path}') as d:
    contents = json.load(d)

## parameters reading from json
config_path = contents['config_path']
connection_profile = contents['connection_profile']
profilename = contents['profilename']
BUCKETNAME = contents['BUCKETNAME']
FOLDERNAME = contents['FOLDERNAME']
TARGET_FOLDERNAME = contents['TARGET_FOLDERNAME']
schemaname = contents['schemaname']
truncate_table = contents['truncate']
chunks = contents['chunks']
table_name = contents['Table_name'].lower()
cols = contents['cols']
logfile = contents['logfile']

# mail notification
Mail_notification = contents['Mail_notification']
dl = Mail_notification['dl']
Subject = Mail_notification['Subject']
s3_connection_failure = Mail_notification['s3_connection_failure']
s3_list_error = Mail_notification['s3_list_error']
s3_get_object_error = Mail_notification['s3_get_object_error']
s3_move_object_error = Mail_notification['s3_move_object_error']
Ingestion_failure = Mail_notification['Ingestion_failure']

#----------------  Establishing the connection to s3 --------------#
s3_client = s3_connect(profilename)

try:
    #############   CREATE THE ENGINE FOR CONNECTING THE HOST   ##############
    DBNAME, USER, PASS, HOST, PORT = read_config_file(config_path, connection_profile)   # calling the function to get configuration params
    engine = sa.create_engine(f"postgresql://{USER}:{PASS}@{HOST}:{PORT}/{DBNAME}")
    cur_date = datetime.today()
    cur_date_now = datetime.now().strftime('%Y_%m_%d-%H_%M')

    ##### Enabling the logging  ######
    with open(f'{logfile}', 'a', newline='') as csvfl:
        write_csv = csv.writer(csvfl, delimiter='|')
        write_csv.writerow(['\t TimeStamp \t', 'Severity', '\t Message \t'])

    logging.basicConfig(filename = logfile,level = logging.INFO, format = ('%(asctime)s | %(levelname)s | %(message)s'))
    logger = logging.getLogger('hvr')

    s3_files_list = s3_list_objects(s3_client,BUCKETNAME,FOLDERNAME)  #list all files in s3

    # Starting the loop
    if len(s3_files_list) > 0 :
        for filename in s3_files_list :    # getting the list of files in s3 folder
            try:
                logging.info(f'current processing file is {os.path.basename(filename)}')
                df = pd.DataFrame()     # intialse empty dataframe
                file_data = s3_get_object(s3_client,BUCKETNAME,filename)   # To get the data from object in s3
                if filename.endswith('.csv'):
                    df = pd.read_csv(BytesIO(file_data.read()),header=0,names=cols,dtype = object)  # load the data into the dataframe
                elif filename.endswith('.xlsx'):
                    df = pd.read_excel(BytesIO(file_data.read()),header=0, names=cols,dtype = object)  # load the data into the dataframe
                df = df.where(pd.notnull(df), None)
                if truncate_table == True:
                    exist_query=f"""SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = '{schemaname}' AND tablename = '{table_name}');"""
                    STATUS = engine.execute(exist_query).fetchall()
                    if(STATUS[0][0] == True or STATUS[0][0] == 't'):
                        truncate_query=f"""truncate table {schemaname}.{table_name}"""
                        engine.execute(truncate_query)
                        logging.info(f"The data in the {schemaname}.{table_name} is truncated")
                df['ingested_filename'] = Path(filename).name
                df['ingestion_timestamp'] = cur_date    # adding the timestamp audit column
                logging.info(f'Data from s3 file retrived and loading to the DataFrame')
                logging.info(f'Total rows and column in file : {df.shape}')
                df.to_sql(table_name,schema = schemaname,con = engine,if_exists = 'append',index = False,method = 'multi',chunksize = chunks) #loading data to the target
                s3_move_object(s3_client,BUCKETNAME,FOLDERNAME,TARGET_FOLDERNAME,filename)   # move file to archival once file loaded to database
                logging.info(f'--- File is loaded to the database and moved---')
            except Exception as e:
                logging.error(f'Exception due to  this error : {e}')
                os.system(f'echo "{Ingestion_failure} . Error Message : {str(e)[:500]} and Please refer to logfile : {logfile}" | mailx -s "{Subject}" {dl}')
    else :
        logging.info(f'Bucket is empty.No files and folders found in the Bucket :{FOLDERNAME}')
except Exception as e :
    logging.error(f'Exception due to the error : {e}')
    os.system(f'echo "{Ingestion_failure} . Error Message : {str(e)[:500]} and Please refer to logfile : {logfile}" | mailx -s "{Subject}" {dl}')

