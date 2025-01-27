import os
import json
import requests
from datetime import datetime
import urllib3
import pandas as pd
import psycopg2
import glob
import psycopg2.extras as extras
import argparse
from utils import get_pg_connection, send_email_notification, setup_logger

def search_request(body):
    global akana_headers
    res = requests.post(
        config["search_url"], headers=akana_headers, verify=False, data=json.dumps(body)
    )
    if res.status_code == requests.codes.unauthorized:
        akana_headers = get_auth_headers()
        res = requests.post(
            config["search_url"], headers=akana_headers, verify=False, data=json.dumps(body)
        )
    return res

def fetchData(app_results):
    extDate = datetime.now().strftime('%d-%m-%Y')
    dff = pd.DataFrame(
        {'u_location_code' :[],'country' :[],'state' :[],'city' :[],'company' :[],'contact' :[],'u_location_id' :[],'u_status' :[],'name' :[],'building_id' :[],'extract_date' :[],'hvr_last_upd_tms' :[]
})
    data=[]
    for item in app_results:
        data.append({'u_location_code': item.get('u_location_code'), 'country': item.get('country'), 'state': item.get('state'),
        'city': item.get('city'),'company': item.get('dv_company'),'contact': item.get('dv_contact'),'u_location_id': item.get('u_location_id'),
        'u_status': item.get('u_status'),'name': item.get('name'),'building_id': item.get('u_hr3_ges_building_id'),'extract_date': extDate})
    main_df=pd.DataFrame(data)
    df=pd.concat([dff,main_df])
    df['extract_date'] = pd.to_datetime(df['extract_date'], format='%d-%m-%Y')
    return df

def get_apps():
    body = {
    "scroll": "20m",
    "size": "1500",
        "sort": ["_doc"],
        "query": {
            "bool": {
            "must": [
            { "match": { "sys_class_name": "location" }},
            ],
            "must_not":[
                {
                    "match":{
                        "u_status": "Retired"
                    }
                }
            ]
            }
        }
    }
    try:
        res_json = search_request(body).json()
        data = res_json['hits']['hits']
        _scroll_id = res_json['_scroll_id']
        app_results = [a["_source"] for a in data]
        reslt = fetchData(app_results)
    except KeyError:
        data = []
        _scroll_id = _scroll_id
    while data:
        bodyScroll = {
            "scroll": "20m",
            "_scroll_id": _scroll_id
        }
        scroll_body = json.dumps(bodyScroll)
        akana_headers = get_auth_headers()
        scroll_res = requests.post(
            config["search_url"], headers=akana_headers, verify=False, data=scroll_body
        )
        try:
            res_json = scroll_res.json()
            data = res_json['hits']['hits']
            _scroll_id = res_json['_scroll_id']
            app_results = [a["_source"] for a in data]
            reslt = pd.concat([reslt,fetchData(app_results)])
        except KeyError:
            _scroll_id = _scroll_id
            logger.error("Error: Elastic Searc Scroll{}".format(str(scroll_res.json())))
    df1 = pd.DataFrame(reslt, columns=config["columns"])
    df1.to_csv(os.path.join(parent_path,'data', 'siteopalconfig_nonretired.csv'), index=False)
    logger.info("csv File generated")

def execute_values(conn, df, table):
    tuples = [tuple(x) for x in df.to_numpy()]
    config["columns"] = ','.join(list(df.columns))
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, config["columns"])
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error("Error- {}".format(error))
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--infile', nargs=1, help="JSON file to be processed", type=argparse.FileType('r'))
    arguments = parser.parse_args()
    config = json.load(arguments.infile[0])
    conn = get_pg_connection(config["config_path"], config["connection_profile"])
    # setting up logger
    parent_path = os.path.dirname(os.path.abspath(__file__))
    log_filename = str(arguments.infile[0].name).split('/')[-1].replace('json', 'log')
    logger = setup_logger(os.path.join(parent_path, "logs", log_filename))
    showpadfiles = glob.glob(os.path.join(parent_path,'data','*'))
    for f in showpadfiles:
        os.remove(f)
    reslt = []
    urllib3.disable_warnings()
    logger.info("Ingestion Started")
    def get_auth_headers():
        request_body = config["request_body"]
        token = requests.post(
            config["auth_url"],
            data=request_body,
            headers={"Accept": "application/json"},
            verify=False,
        ).json()
        headers = {
            "Authorization": "{} {}".format(token["token_type"], token["access_token"]),
            "x-api-key": config["x-api-key"]
        }
        return headers
    try:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        akana_headers = get_auth_headers()
        get_apps()
        df = pd.read_csv(os.path.join(parent_path,"data",'siteopalconfig_nonretired.csv'),low_memory=False,)
        df["extract_date"] = pd.to_datetime(df["extract_date"])
        df = df.where(pd.notnull(df), None)
        df['hvr_last_upd_tms'] = datetime.now()
        execute_values(conn, df, config["schema_name"]+'.'+config["table_name"])
        logger.info(f'Data inserted to {config["schema_name"]}.{config["table_name"]}')
        logger.info("Ingestion Completed")
    except Exception as e:
      logger.error(f"some error occurred while executing {e}")
      send_email_notification(f"Exception occured {e} at {parent_path}", "Site Opal Ingestion Failure|Prod")

