import configparser
import psycopg2
import os
import json
import requests
import glob
from boxsdk import JWTAuth, Client
from datetime import datetime, timezone, timedelta
import pandas as pd
import psycopg2
import sys
import psycopg2.extras as extras

def read_config_file(filepath, connection):
    config = configparser.ConfigParser()
    config.read(filepath)
    db_name = config[connection]['dbname']
    user = config[connection]['user']
    password = config[connection]['password']
    host = config[connection]['host']
    port = int(config[connection]['port'])
    return db_name, user, password, host, port

def getAccessToBox(JWT_file_path):
    print("START at", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), ":", "getAccessToBox(",
          "JWT_file_path=", JWT_file_path, ")")
    auth = JWTAuth.from_settings_file(JWT_file_path)
    client = Client(auth)
    print("END at", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), ":", "getAccessToBox")
    return client
def getFileWithChangeName(client, file_id, dest_location, box_folder_id):
    print("START at", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), ":", "getFileWithChangeName(", "client",
          ", file_id=", file_id, ", dest_location=", dest_location, ", box_folder_id=", box_folder_id, ")")
    file_name = client.file(file_id).get().name.replace(' ', '_').replace('-', '_')
    file_name1 = file_name[:-5]
    open_file = open(dest_location + file_name, 'wb')
    client.file(file_id).download_to(open_file)
    open_file.close()
    df = pd.read_excel(targetpath + file_name, engine='openpyxl')
    df.columns = df.columns.str.lower()
    df.to_csv(targetpath + file_name1 + '.csv', index=False)
    os.remove(targetpath + file_name)

def execute_values(conn, df, table):
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("the dataframe is inserted")
    cursor.close()


def getFiles(client, folder_id, Files_Stage_location, file_name_pattern):
    filesLoaded = list()
    for item in client.folder(folder_id).get_items():
        if item.type == "file":
            getFileWithChangeName(client, item.id, Files_Stage_location, folder_id)
            filesLoaded.append(item)
    print("END at", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), ":", "getFiles")
    return filesLoaded

if __name__ == "__main__":
    try:
        arg1 = arg1 = sys.argv[1]
        with open(arg1) as user_file:
            file_contents = user_file.read()
        parsed_json = json.loads(file_contents)

        boxfolderid = parsed_json['box_id']
        targetpath = parsed_json['targetpath']
        configpath = parsed_json['config_path']
        filename = parsed_json['file_name']
        schemaname = parsed_json['schema_name']
        tablename = parsed_json['table_name']
        ingestiondl = parsed_json['targetpath']
        dbconfig_path = parsed_json['dbconfig_path']
        connection_profile = parsed_json['connection_profile']
        dl=parsed_json['dl']
        DBNAME, USER, PASS, HOST, PORT = read_config_file(dbconfig_path, connection_profile)
        client = getAccessToBox(configpath)
        getFiles(client, boxfolderid, targetpath, f'*')
        os.chdir(targetpath)
        for file in glob.glob("*.csv"):
            df = pd.read_csv(targetpath + filename, encoding='cp1252')
            df.columns = df.columns.str.replace('/', '_')
            df.columns = df.columns.str.replace(' ', '_')
            df.columns = df.columns.str.replace('__', '_')
            df.columns = df.columns.str.lower()
            df['ingestion_timestamp'] = datetime.now()
            df = df.where(pd.notnull(df), None)
            df= df.dropna(how='all',axis='columns')
            print(df.columns)
            conn = psycopg2.connect(dbname=DBNAME, user=USER, password=PASS, host=HOST, port=PORT)
            # Q1 = 'truncate table ' + schemaname + '.' + tablename
            # cursor = conn.cursor()
            # cursor.execute(Q1)
            execute_values(conn, df, schemaname + '.' + tablename)
            print("Successfully data loaded to RS")

    except Exception as e:
        print(e)
        os.system(
            f'echo "Exception from the archived files" | mailx -s "some error occurred while executing" {dl} ')
