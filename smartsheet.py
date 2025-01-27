import requests,json,csv
import pandas as pd
import configparser
from datetime import date,datetime, timedelta
import sqlalchemy as sa
from sqlalchemy import types
import logging
import sys
import os

ARGV=sys.argv
config_path = ARGV[1]

with open(f'{config_path}') as d :
    data=json.load(d)

####Source details
Source=data['Source']
id_lst = Source['id']
token=Source['token']
URL =Source['url']

#####Target details
Target = data['Target']
config_path = Target['config_path']
connection_profile = Target['connection_profile']
datapath = Target['datapath']
schema_name = Target['schema_name']
Tablenames = Target['Tablenames']
chunk = Target['chunksize']

###### Touchfile requirement
Touchfile = data['Touchfile']
Touchfile_required = Touchfile['Touchfile_required']
touchfile = Touchfile['touchfile']
projectname = Touchfile['projectname']
s3path = Touchfile['s3path']
s3_profile = Touchfile['s3_profile']

#### Email info
Email_info = data['Email_info']
dl = Email_info['dl']
subject = Email_info['subject']
Message_Ingestion = Email_info['Message_Ingestion']

def read_config_file(filepath, connection):
        config = configparser.ConfigParser()
        config.read(filepath)
        db_name = config[connection]['dbname']
        user = config[connection]['user']
        password = config[connection]['password']
        host = config[connection]['host']
        port = int(config[connection]['port'])

        return db_name,user,password,host,port

#######logfile #####
logfile = data['logfile']

############# Logging #########
with open(f'{logfile}', 'a', newline='') as csvfl:
    write_csv = csv.writer(csvfl, delimiter='|')
    write_csv.writerow(['\t TimeStamp \t', 'Severity', '\t Message \t'])

logging.basicConfig(filename=logfile,level=logging.INFO, format=('%(asctime)s | %(levelname)s | %(message)s'))
logger = logging.getLogger('hvr')



try :
    ######## calling Connections Method from dbconnections
    DBNAME, USER, PASS, HOST, PORT = read_config_file(config_path,connection_profile)
    engine = sa.create_engine(f"postgresql://{USER}:{PASS}@{HOST}:{PORT}/{DBNAME}")
    conn = engine.connect()
    conn.autocommit = True
    logging.info('Database connection successful')
    curt_date  = datetime.today()
    cur_date=datetime.now().strftime('%Y_%m_%d-%H_%M')

    ######reading token and url from root
   # Token,URL = smartsheets_details(smartsheet_path)
   # token=Token


    #### Getting data from Smartsheet url
    proxy = { "https": "https://http-proxy.health.ge.com:88"}
    hedrs = { "Authorization" : f"Bearer {token}", "Accept": "text/csv"}

    for id,tablename in zip(id_lst,Tablenames):
        try:
            url = f"{URL}{id}"
            response = requests.get(url, headers=hedrs)
            response.raise_for_status()
            sheet = response.content
            sheetname = f"{tablename}"
            logging.info(sheetname)
            with open(f"{datapath}{sheetname}.csv", 'wb+') as f:
                f.write(sheet)
            logging.info(f'Data from smartsheet copied to file : {sheetname}.csv')
        except Exception as e :
            logging.error(f'Exception due to this error : {e}')
            os.system(f'echo "{Message_Ingestion}{logfile}" | mailx -s "{subject}"  {dl}')

    for tablename in Tablenames :
        try:
            df = pd.read_csv(f"{datapath}{tablename}.csv",parse_dates=True,keep_date_col = True)
            df = df.where(pd.notnull(df), None)
            df["ingestion_timestamp"]=curt_date
            tablename=tablename.lower()
            exist_query=f"""SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = '{schema_name}' AND tablename = '{tablename}');"""
            STATUS = engine.execute(exist_query).fetchall()
            if(STATUS[0][0] == True or STATUS[0][0] == 't'):
                truncate_query=f"""truncate table {schema_name}.{tablename}"""
                engine.execute(truncate_query)
                logging.info(f"the data in the {schema_name}.{tablename} is truncated ")
            df.to_sql(f"{tablename}",schema=schema_name,con=conn,if_exists='append',index=False,method='multi',chunksize=1000)
            logging.info(f'data is loaded into redshift : {schema_name}.{tablename}')
            if Touchfile_required.lower() == 'yes':
                os.system(f'touch {touchfile}redshift-{projectname}-{tablename}-{cur_date}.csv')
                os.system(f'aws s3 cp {touchfile}redshift-{projectname}-{tablename}-{cur_date}.csv {s3path} --profile {s3_profile}')
        except Exception as e :
            logging.error(f'Exception due to this error : {e}')
            os.system(f'echo "{Message_Ingestion}{e}{logfile}" | mailx -s "{subject}" {dl}')
except Exception as e:
    print(e)
    os.system(f'echo "{Message_Ingestion}{e}{logfile}" | mailx -s "{subject}" {dl}')
