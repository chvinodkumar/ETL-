import configparser
import psycopg2
import sqlalchemy as sa

config_path = '/home/hvr/.aws/redshift_connection.ini'
connection_profile = 'Connections_PROD'


# Pick from the following
# ['Connections_PROD', 'Connections_INNOVATION', 'Connections_FINNPROD', 'Connections_FINPROD']
def read_config_file(filepath, connection):
    config = configparser.ConfigParser()
    config.read(filepath)
    db_name = config[connection]['dbname']
    user = config[connection]['user']
    password = config[connection]['password']
    host = config[connection]['host']
    port = int(config[connection]['port'])
    return db_name, user, password, host, port


DBNAME, USER, PASS, HOST, PORT = read_config_file(config_path, connection_profile)


import os
import json
import requests
from datetime import datetime
import numpy as np
import urllib3
import pandas as pd
import psycopg2
import glob
import psycopg2.extras as extras
from sqlalchemy import create_engine



filePath='/home/hvr/opalconfig/data/'

showpadfiles = glob.glob(filePath+'*')
for f in showpadfiles:
    os.remove(f)

extDate=datetime.now().strftime('%d-%m-%Y')
logs = []
reslt=[]



def get_auth_headers():
    request_body = {
        "client_id": 'HC-Asset',
        "client_secret": '0e0065acd1e1b91e5922a07979d78bbbd12b95af',
        "scope": "api",
        "grant_type": "client_credentials",
    }

    token = requests.post(
        "https://fssfed.ge.com/fss/as/token.oauth2",
        data=request_body,
        headers={ "Accept": "application/json" },
        verify=False,
    ).json()

    headers = {
        "Authorization": "{} {}".format(token["token_type"], token["access_token"]),
        "x-api-key": "LHnZRBtyRA1jFyS7YdjqE9rNE4g0st2G1lKIrTVj"
    }

    return headers

def search_request(body):
    global akana_headers
    res = requests.post(
        search_url, headers=akana_headers, verify=False, data=json.dumps(body)
    )
    if res.status_code == requests.codes.unauthorized:
        akana_headers = get_auth_headers()
        res = requests.post(
            search_url, headers=akana_headers, verify=False, data=json.dumps(body)
        )
    print(res)
    return res


cols = ['sys_class_name', 'u_config_item_id', 'sys_id', 'name', 'company', 'u_business_segment', 'department',
        'department_id', 'support_group', 'operational_status', 'install_date', 'model_manufacturer',
        'model_classification', 'u_function',
        'location_sys_id', 'u_site', 'country', 'data_origin', 'posting_agent', 'load_dtm']


def fetchData(app_results):
    df = pd.DataFrame(
        {'sys_class_name': [], 'u_config_item_id': [], 'sys_id': [], 'name': [], 'company': [],
         'u_business_segment': [], 'department': [],
         'department_id': [], 'support_group': [], 'operational_status': [], 'install_date': [],
         'model_manufacturer': [],
         'model_classification': [], 'u_function': [],
         'location_sys_id': [], 'u_site': [], 'country': [], 'data_origin': [], 'posting_agent': [], 'load_dtm': []})
    for item in app_results:
        df = df.append(
            {'sys_class_name': item.get('sys_class_name'), 'u_config_item_id': item.get('u_config_item_id'),
             'sys_id': item.get('sys_id'), 'name': item.get('name'),
             'company': item.get('company'),
             'u_business_segment': item.get('u_business_segment'), 'department': item.get('department'),
             'department_id': str(item.get('department_id')), 'support_group': item.get('support_group'),
             'operational_status': item.get('operational_status'), 'install_date': item.get('install_date'),
             'model_manufacturer': item.get('model_manufacturer'),
             'model_classification': item.get('model_classification'), 'u_function': item.get('u_function'),
             'location_sys_id': item.get('location_sys_id'), 'u_site': item.get('u_site'),
             'country': item.get('country'), 'data_origin': 'OPAL API',
             'posting_agent': 'opalconfig.py', 'load_dtm': datetime.now()}, ignore_index=True)
    return df





def get_apps():
    body = {
"scroll": "20m",
  "size": "1500",
    "sort": ["_doc"],
      "query": {
        "bool": {
            "should": [
                {
                    "match": {
                        "sys_class_name": "u_cmdb_ci_business_app"
                    }
                },
                {
                    "match": {
                        "sys_class_name": "cmdb_ci_service"
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
        reslt=fetchData(app_results)
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
        search_url, headers=akana_headers, verify=False, data=scroll_body
        )
        print(scroll_res)
        try:
            res_json = scroll_res.json()
            data = res_json['hits']['hits']
            _scroll_id = res_json['_scroll_id']
            app_results = [a["_source"] for a in data]
            reslt=reslt.append(fetchData(app_results))
        except KeyError:
            _scroll_id = _scroll_id
            print ('Error: Elastic Search Scroll: %s' % str(scroll_res.json()))

    df1 = pd.DataFrame(reslt,columns = cols)

    df1.to_csv(filePath+'opalconfig.csv',index = False)


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




if __name__ == "__main__":
    try:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        akana_headers = get_auth_headers()
        search_url = "https://api.ge.com/digital/opal/v3/assets/elastic/search"
        start = datetime.now()
        get_apps()
        #conn = psycopg2.connect(dbname='gehc_data', user='502835360', password='OMi7oM#TY#a', host='us-prod-etl-alpha-redshift.cjn5iynjwss7.us-east-1.redshift.amazonaws.com', port=5439)
        conn = psycopg2.connect(dbname=DBNAME, user=USER, password=PASS, host=HOST, port=PORT)
        df = pd.read_csv(filePath + 'opalconfig.csv')
        #df.fillna('null', inplace=True)
        df = df.where(pd.notnull(df), None)
        Q1 = f"""truncate table opal_fr.hc_dtb_opal_config_f"""
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(Q1)
        execute_values(conn, df, 'opal_fr.hc_dtb_opal_config_f')

    except Exception as e:
        print(e)

###################################
import configparser
import psycopg2
import sqlalchemy as sa
from datetime import datetime
config_path = '/home/hvr/.aws/redshift_connection.ini'
connection_profile = 'Connections_PROD'


# Pick from the following
# ['Connections_PROD', 'Connections_INNOVATION', 'Connections_FINNPROD', 'Connections_FINPROD']
def read_config_file(filepath, connection):
    config = configparser.ConfigParser()
    config.read(filepath)
    db_name = config[connection]['dbname']
    user = config[connection]['user']
    password = config[connection]['password']
    host = config[connection]['host']
    port = int(config[connection]['port'])
    return db_name, user, password, host, port


DBNAME, USER, PASS, HOST, PORT = read_config_file(config_path, connection_profile)

import os
import json
import requests
from datetime import datetime
import numpy as np
import urllib3
import pandas as pd
import psycopg2
import glob
import psycopg2.extras as extras
from sqlalchemy import create_engine



filePath = '/home/hvr/hcssopalincidentf/data/'

showpadfiles = glob.glob(filePath + '*')
for f in showpadfiles:
    os.remove(f)

extDate = datetime.now().strftime('%d-%m-%Y')
logs = []
reslt = []


def get_auth_headers():
    request_body = {
        "client_id": 'HC-Asset',
        "client_secret": '0e0065acd1e1b91e5922a07979d78bbbd12b95af',
        "scope": "api",
        "grant_type": "client_credentials",
    }

    token = requests.post(
        "https://fssfed.ge.com/fss/as/token.oauth2",
        data=request_body,
        headers={"Accept": "application/json"},
        verify=False,
    ).json()

    headers = {
        "Authorization": "{} {}".format(token["token_type"], token["access_token"]),
        "x-api-key": "LHnZRBtyRA1jFyS7YdjqE9rNE4g0st2G1lKIrTVj"
    }

    return headers


def search_request(body):
    global akana_headers
    res = requests.post(
        search_url, headers=akana_headers, verify=False, data=json.dumps(body)
    )
    if res.status_code == requests.codes.unauthorized:
        akana_headers = get_auth_headers()
        res = requests.post(
            search_url, headers=akana_headers, verify=False, data=json.dumps(body)
        )
    print(res)
    return res


cols = ['sys_class_name', 'u_service', 'u_service_sys_id', 'number', 'state', 'priority', 'opened_at', 'closed_at',
        'resolved_at', 'close_notes', 'close_code', 'short_description', 'description', 'business_duration_seconds',
        'calendar_duration_seconds', 'attr_seconds', 'dttr_seconds', 'business_impact', 'assignment_group',
        'assigned_to', 'outage_owner', 'caused_by', 'opened_by', 'cause', 'resolved_by', 'closed_by', 'category',
        'company', 'company_sys_id', 'sys_id', 'problem_id', 'inc_sys_id', 'cmdb_ci_sys_id', 'cmdb_ci', 'u_environment',
        'u_environment_sys_id', 'urgency', 'subcategory', 'hold_reason', 'data_origin', 'posting_agent', 'load_dtm']


def fetchData(app_results):
    df = pd.DataFrame(
        {'sys_class_name': [], 'u_service': [], 'u_service_sys_id': [], 'number': [], 'state': [], 'priority': [],
         'opened_at': [], 'closed_at': [], 'resolved_at': [], 'close_notes': [], 'close_code': [],
         'short_description': [], 'description': [], 'business_duration_seconds': [], 'calendar_duration_seconds': [],
         'attr_seconds': [], 'dttr_seconds': [], 'business_impact': [], 'assignment_group': [], 'assigned_to': [],
         'outage_owner': [], 'caused_by': [], 'opened_by': [], 'cause': [], 'resolved_by': [], 'closed_by': [],
         'category': [], 'company': [], 'company_sys_id': [], 'sys_id': [], 'problem_id': [], 'inc_sys_id': [],
         'cmdb_ci_sys_id': [], 'cmdb_ci': [], 'u_environment': [], 'u_environment_sys_id': [], 'urgency': [],
         'subcategory': [], 'hold_reason': [], 'data_origin': [], 'posting_agent': [], 'load_dtm': []})
    for item in app_results:
        if item.get('sys_class_name')=='incident' and item.get('company')=='GE Healthcare' and item.get('u_service')=='Service Suite' or item.get('u_service')=='D&A ODP US GXP - Platform Spotfire':
            df = df.append(
                {'sys_class_name': item.get('sys_class_name'), 'u_service': item.get('u_service'),
                 'u_service_sys_id': item.get('u_service_sys_id'), 'number': item.get('number'), 'state': item.get('state'),
                 'priority': item.get('priority'), 'opened_at': item.get('opened_at'), 'closed_at': item.get('closed_at'),
                 'resolved_at': item.get('resolved_at'), 'close_notes': item.get('close_notes'),
                 'close_code': item.get('close_code'), 'short_description': item.get('short_description'),
                 'description': item.get('description'), 'business_duration_seconds': item.get('business_duration_seconds'),
                 'calendar_duration_seconds': item.get('calendar_duration_seconds'), 'attr_seconds': item.get('attr_seconds'),
                 'dttr_seconds': item.get('dttr_seconds'), 'business_impact': item.get('business_impact'),
                 'assignment_group': item.get('assignment_group'), 'assigned_to': item.get('assigned_to'),
                 'outage_owner': item.get('outage_owner'), 'caused_by': item.get('caused_by'), 'opened_by': item.get('opened_by'),
                 'cause': item.get('cause'), 'resolved_by': item.get('resolved_by'), 'closed_by': item.get('closed_by'),
                 'category': item.get('category'), 'company': item.get('company'), 'company_sys_id': item.get('company_sys_id'),
                 'sys_id': item.get('sys_id'), 'problem_id': item.get('problem_id'), 'inc_sys_id': item.get('inc_sys_id'),
                 'cmdb_ci_sys_id': item.get('cmdb_ci_sys_id'), 'cmdb_ci': item.get('cmdb_ci'),
                 'u_environment': item.get('u_environment'), 'u_environment_sys_id': item.get('u_environment_sys_id'),
                 'urgency': item.get('urgency'), 'subcategory': item.get('subcategory'), 'hold_reason': item.get('hold_reason'), 'data_origin': 'opal_fr',
                 'posting_agent': 'hcssopalincident.py',
                 'load_dtm': datetime.now()}, ignore_index=True)
    return df


def get_apps():
    body = {
        "scroll": "20m",
        "size": "1500",
        "sort": ["_doc"],
        "query": {
            "bool": {
            "must": [{"match": {"sys_class_name": "incident"}},
                     {"match": {"u_service": "Service Suite','D&A ODP US GXP - Platform Spotfire"}},
                     {"match": {"company": "GE Healthcare"}}]
        }
        }
    }


    try:
        res_json = search_request(body).json()
        data = res_json['hits']['hits']
        _scroll_id = res_json['_scroll_id']
        app_results = [a["_source"] for a in data]
        reslt = fetchData(app_results)
        totalcount = len(reslt)
        print('Count is :' + str(totalcount))
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
            search_url, headers=akana_headers, verify=False, data=scroll_body
        )
        print(scroll_res)
        try:
            res_json = scroll_res.json()
            data = res_json['hits']['hits']
            _scroll_id = res_json['_scroll_id']
            app_results = [a["_source"] for a in data]
            reslt = reslt.append(fetchData(app_results))
            totalcount = len(reslt)
            print('Count is :' + str(totalcount))
        except KeyError:
            _scroll_id = _scroll_id
            print('Error: Elastic Search Scroll: %s' % str(scroll_res.json()))
    print('res')
    df1 = pd.DataFrame(reslt, columns=cols)
    df1.to_csv(filePath + 'hcssopalincidentf.csv', index=False)


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


if __name__ == "__main__":
    try:
        current_datetime=datetime.now()
        print("Current run date and time:",current_datetime)
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        akana_headers = get_auth_headers()
        search_url = "https://api.ge.com/digital/opal/v3/assets/elastic/search"
        start = datetime.now()
        get_apps()
        conn = psycopg2.connect(dbname=DBNAME, user=USER, password=PASS, host=HOST, port=PORT)
        df = pd.read_csv(filePath + 'hcssopalincidentf.csv')
        df = df.where(pd.notnull(df), None)
        print(df.columns)
        Q1 = f"""truncate table opal_fr.hc_ss_opal_incident_f"""
        cursor = conn.cursor()
        cursor.execute(Q1)
        execute_values(conn, df, 'opal_fr.hc_ss_opal_incident_f')
        print("Successfully data loaded to RS")
        print("success")

    except Exception as e:
        print(e)

#################################
[root@ip-10-242-109-196 /]# cat /home/hvr/jifflenow/test.py
import os
import requests
import json
import sys
from datetime import datetime,timedelta
from json.decoder import JSONDecodeError
import glob
from urllib.parse import urlencode
import pandas as pd

tokenfile = 'tokens.json'
client_id = 'd0c86b6171bbc7336b5ec33559b75116d8ef3bb0634535faab3a1d54b8fdeecc'
redirect_uri = 'https://sample_url'
jiffle_authorize_url = 'https://gehealthcare.jifflenow.com/api/oauth/authorize'
client_secret = '7c4dade94520321f26a54f4f3628275700b7372b22312e12e6e118bb7ea3cede'
token_endpoint_url = 'https://gehealthcare.jifflenow.com/api/oauth/token'
events_list_url = 'https://gehealthcare.jifflenow.com/api/adapter/events_list'
baseurl = 'https://gehealthcare.jifflenow.com/api/adapter/'

eventfilepath = f'/home/hvr/jifflenow/data/events.json'
eventpath = f'/home/hvr/jifflenow/data/events/'
meetingsfilepath = f'/home/hvr/jifflenow/data/mettings/'
meetingsbyuserfilepath = f'/home/hvr/jifflenow/data/meetingsbyuser/'
sessionsfilepath = f'/home/hvr/jifflenow/data/sessions/'
dirpath = f'/home/hvr/jifflenow/'

# eventfilepath = f'D:\\jiffle\\data\\events.json'
# eventpath = f'D:\jiffle\\data\\events\\'
# meetingsfilepath = f'D:\\jiffle\\data\\mettings\\'
# meetingsbyuserfilepath = f'D:\\jiffle\\data\\meetingsbyuser\\'
# sessionsfilepath = f'D:\\jiffle\\data\\sessions\\'
# dirpath = f'D:\\jiffle\\'

def hdr(access_token):
    hdr = {'Content-Type': 'application/json',
     'Authorization': 'Bearer ' + access_token,
     'X-Integration-User-ID': '9a1ebff0eb181a222aaeec684f9d8b68ec6d45f96af399dadb118e4089a966a2@integration.gehealthcare.com'
     }
    return hdr

def get_code() -> str:
    query = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": 'code'
    }

    try:
        response = requests.get(jiffle_authorize_url + '?' + urlencode(query))
        response.raise_for_status()
    except requests.exceptions.ConnectionError as err:
        code = str(err).split('code=', 1)[1].split(' ', 1)[0]
    return code


def get_events_list(access_token: str) -> requests.Response:
    query = {
        "api_params[past_events]": True
    }
    events_list_query_url = events_list_url + '?' + urlencode(query)
    headers = hdr(access_token)
    print(events_list_query_url)
    response = requests.get(events_list_query_url, headers=headers)
    return response


def refresh_tokens(refresh_token):
    query = {
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
        "grant_type": 'refresh_token',
        "refresh_token": refresh_token
    }

    refresh_token_url = token_endpoint_url + '?' + urlencode(query)
    response = requests.post(refresh_token_url)

    if response.status_code != 200:
        code = get_code()
        url_authorize = 'https://gehealthcare.jifflenow.com/api/oauth/token?client_id=d0c86b6171bbc7336b5ec33559b75116d8ef3bb0634535faab3a1d54b8fdeecc' \
                        '&client_secret=7c4dade94520321f26a54f4f3628275700b7372b22312e12e6e118bb7ea3cede' \
                        '&code=' + code + '&grant_type=authorization_code&redirect_uri=https://sample_url'
        response2 = requests.post(url_authorize)
        accesstoken = response2.json()['access_token']
        refreshtoken = response2.json()['refresh_token']
        return accesstoken, refreshtoken

    new_access_token = response.json()['access_token']
    new_refresh_token = response.json()['refresh_token']
    return new_access_token, new_refresh_token


def save_to_path(accesstoken, refresh_token):
    onlyevts = glob.glob(dirpath + '*.json')
    for f5 in onlyevts:
        os.remove(f5)
    tokens = {"access_token": accesstoken, "refresh_token": refresh_token}
    with open(dirpath + tokenfile, 'w') as file:
        json.dump(tokens, file)


def load_from_path():
    try:
        with open(dirpath + tokenfile) as file:
            tokens = json.load(file)
    except JSONDecodeError as e:
        print('Decoding JSON has failed', e)
        return None
    except FileNotFoundError as e:
        print(f"File {dirpath + tokenfile} not found!", file=sys.stderr)
        return None
    return tokens["refresh_token"]

def meetings(flat_pred):
    textfile1 = open(meetingsfilepath + eventsystemname + '.json', "w", encoding="utf-8")
    textfile1.write(json.dumps(flat_pred))
    textfile1.close()
    with open(meetingsfilepath + eventsystemname + '.json', encoding='utf-8') as inputfile:
        df = pd.read_json(inputfile)
    df.to_csv(meetingsfilepath + eventsystemname + '.csv', encoding='utf-8', index=False)
    with open(meetingsfilepath + eventsystemname + '.csv', encoding='utf-8') as inputfile2:
        df2 = pd.read_csv(inputfile2)
        df2['event_system_name'] = eventsystemname
        df2=df2.astype(str)
        df2['op_val'] = 2
        df2['ingestion_timestamp'] = datetime
    df2.to_parquet(meetingsfilepath + eventsystemname + '.parquet')

def meetingsbyuser(meetinguser):
        textfile2 = open(meetingsbyuserfilepath + eventsystemname + '.json', "w", encoding="utf-8")
        textfile2.write(json.dumps(meetinguser))
        textfile2.close()
        with open(meetingsbyuserfilepath + eventsystemname + '.json', encoding='utf-8') as inputfile1:
            df = pd.read_json(inputfile1)
        df.to_csv(meetingsbyuserfilepath + eventsystemname + '.csv', encoding='utf-8', index=False)
        with open(meetingsbyuserfilepath + eventsystemname + '.csv', encoding='utf-8') as inputfile2:
            df2 = pd.read_csv(inputfile2)
            df2['event_system_name'] = eventsystemname
            df2=df2.astype(str)
            df2['op_val'] = 2
            df2['ingestion_timestamp'] = datetime
        df2.to_parquet(meetingsbyuserfilepath + eventsystemname + '.parquet')


def usersessions(session):
    if len(session.json()['data']['sessions']) > 0:
        print(len(session.json()['data']['sessions']))
        dd = session.json()['data']['sessions']
        with open(sessionsfilepath + eventsystemname + '.json', "w+") as outfile:
            json.dump(dd, outfile)
        with open(sessionsfilepath + eventsystemname + '.json') as inputfile3:
            df = pd.read_json(inputfile3)
        df.to_csv(sessionsfilepath + eventsystemname + '.csv', encoding='utf-8', index=False)
        with open(sessionsfilepath + eventsystemname + '.csv', encoding='utf-8') as inputfile2:
            df2 = pd.read_csv(inputfile2)
            df2['event_system_name'] = eventsystemname
            df2=df2.astype(str)
            df2['op_val'] = 2
            df2['ingestion_timestamp'] = datetime
        df2.to_parquet(sessionsfilepath + eventsystemname + '.parquet')



path = eventpath
events = glob.glob(path+'*.*')
for f in events:
    os.remove(f)

path1 = meetingsfilepath
mettingsf = glob.glob(path1+'*.*')
for f1 in mettingsf:
    os.remove(f1)

path2 = meetingsbyuserfilepath
mettingsbyusrf = glob.glob(path2+'*.*')
for f2 in mettingsbyusrf:
    os.remove(f2)

path3 = sessionsfilepath
sessionsf = glob.glob(path3+'*.*')
for f3 in sessionsf:
    os.remove(f3)


now = datetime.now()- timedelta(days=1)
now1 = datetime.now()
print(now.strftime("%Y-%m-%d T %H-%M-%s"))
dt_out = now.strftime("%Y-%m-%d")
datetime = now1.strftime("%Y-%m-%dT%H-%M-%S")
timestamp = '&api_params[last_sync_time]='+dt_out+''

oldrefresh_token = load_from_path()
accesstoken, refreshtoken = refresh_tokens(oldrefresh_token)
save_to_path(accesstoken, refreshtoken)
response = get_events_list(accesstoken)
hdr = hdr(accesstoken)
textfile = open(eventfilepath, "w", encoding="utf-8")
print(f'programs - writing to file... {eventfilepath}')
textfile.write(json.dumps(response.json()['data']['events']))
textfile.close()
with open(eventfilepath, encoding='utf-8') as inputfile:
    df = pd.read_json(inputfile)
df.to_csv(eventpath + 'events.csv', encoding='utf-8', index=False)
with open(eventpath + 'events.csv', encoding='utf-8') as inputfile2:
    df2 = pd.read_csv(inputfile2)
    df2 = df2.astype({"event_name":str,"event_system_name":str,"start_date":str,"end_date":str,"location":str,"time_zone":str,"status":str,"stage":str,"event_type":str})
    df2['op_val'] = 2
    df2['ingestion_timestamp'] = datetime
df2.to_parquet(eventpath + 'events.parquet')


eventlist = response.json()['data']['events']
for eventitem in eventlist:
    eventsystemname = eventitem['event_system_name']
    meetingsurl = baseurl + eventsystemname + '/entity/meetings?api_params[per_page]=100&api_params[page]=1'+timestamp
    response4 = requests.get(meetingsurl, headers=hdr)
    if response4.status_code == 200 and len(response4.json()['data']) != 0:
        totalpages = response4.json()['data']['total_pages']
        totalmeetingsarray = []
        for i in range(totalpages):
            page = int(i) + 1
            meetingsurl = baseurl + eventsystemname + '/entity/meetings?api_params[per_page]=100&api_params[page]=' + str(page)+timestamp
            print(meetingsurl)
            meetingres = requests.get(meetingsurl, headers=hdr)
            if meetingres.status_code == 200 and len(meetingres.json()['data']) != 0:
                if len(meetingres.json()['data']['meetings']) > 0:
                    print(len(meetingres.json()['data']['meetings']))
                    totalmeetingsarray.append(meetingres.json()['data']['meetings'])
            elif meetingres.status_code == 401:
                oldrefresh_token = load_from_path()
                accesstoken, refreshtoken = refresh_tokens(oldrefresh_token)
                save_to_path(accesstoken, refreshtoken)
                meetingsurl = baseurl + eventsystemname + '/entity/meetings?api_params[per_page]=100&api_params[page]=' + str(page)+timestamp
                hdr = {'Content-Type': 'application/json',
                       'Authorization': 'Bearer ' + accesstoken,
                       'X-Integration-User-ID': '9a1ebff0eb181a222aaeec684f9d8b68ec6d45f96af399dadb118e4089a966a2@integration.gehealthcare.com'
                       }
                meetingres1 = requests.get(meetingsurl, headers=hdr)
                if meetingres1.status_code == 200 and len(meetingres1.json()['data']) != 0:
                    if len(meetingres1.json()['data']['meetings']) > 0:
                        print(len(meetingres1.json()['data']['meetings']))
                        totalmeetingsarray.append(meetingres1.json()['data']['meetings'])
        if len(totalmeetingsarray) > 0:
            flat_pred = [item for sublist in totalmeetingsarray for item in sublist]
            meetings(flat_pred)

    eventsystemname = eventitem['event_system_name']
    meetingsbyuserurl = baseurl + eventsystemname + '/meetings_list?api_params[all]=true&api_params[per_page]=100&api_params[page]=1'+timestamp
    print(meetingsbyuserurl)
    response5 = requests.get(meetingsbyuserurl, headers=hdr)
    if response5.status_code == 200 and len(response5.json()['data']) != 0:
        meetbyusrtotalpgs = response5.json()['meta']['total_pages']
        totalmeetingsnbyuserarray = []
        for i in range(meetbyusrtotalpgs):
            page = int(i) + 1
            meetingsbyuserurl = baseurl + eventsystemname + '/meetings_list?api_params[all]=true&api_params[per_page]=100&api_params[page]='+str(page)+timestamp
            print(meetingsbyuserurl)
            meetingbyuserres = requests.get(meetingsbyuserurl, headers=hdr)
            if meetingbyuserres.status_code == 200 and len(meetingbyuserres.json()['data']) != 0:
                if len(meetingbyuserres.json()['data']['users']) > 0:
                    print(len(meetingbyuserres.json()['data']['users']))
                    totalmeetingsnbyuserarray.append(meetingbyuserres.json()['data']['users'])
            elif response5.status_code == 401:
                print('Meetings UnAuthrzied')
                oldrefresh_token = load_from_path()
                accesstoken, refreshtoken = refresh_tokens(oldrefresh_token)
                save_to_path(accesstoken, refreshtoken)
                meetingsbyuserurl = baseurl + eventsystemname + '/meetings_list?api_params[all]=true&api_params[per_page]=100&api_params[page]='+str(page)+timestamp

                hdr = {'Content-Type': 'application/json',
                       'Authorization': 'Bearer ' + accesstoken,
                       'X-Integration-User-ID': '9a1ebff0eb181a222aaeec684f9d8b68ec6d45f96af399dadb118e4089a966a2@integration.gehealthcare.com'
                       }
                print(meetingsbyuserurl)
                meetingbyuserres = requests.get(meetingsbyuserurl, headers=hdr)
                if meetingbyuserres.status_code == 200 and len(meetingbyuserres.json()['data']) != 0:
                    if len(meetingbyuserres.json()['data']['users']) > 0:
                        print(len(meetingbyuserres.json()['data']['users']))
                        totalmeetingsnbyuserarray.append(meetingbyuserres.json()['data']['users'])
        if len(totalmeetingsnbyuserarray)>0:
            flat_pred = [item for sublist in totalmeetingsnbyuserarray for item in sublist]
            meetingsbyuser(flat_pred)

    eventsystemname = eventitem['event_system_name']
    sessionsurl = baseurl + eventsystemname + '/entity/sessions?api_params[past_events]=true'+timestamp
    print(sessionsurl)
    response6 = requests.get(sessionsurl, headers=hdr)
    if response6.status_code == 200 and len(response6.json()['data']) != 0:
        usersessions(response6)
    elif response6.status_code == 401:
        print('Sessions UnAuthrzied')
        oldrefresh_token = load_from_path()
        accesstoken, refreshtoken = refresh_tokens(oldrefresh_token)
        save_to_path(accesstoken, refreshtoken)
        sessionsurl = 'https://gehealthcare.jifflenow.com/api/adapter/' + eventsystemname + '/entity/sessions?api_params[past_events]=true'+timestamp
        hdr = {'Content-Type': 'application/json',
               'Authorization': 'Bearer ' + accesstoken,
               'X-Integration-User-ID': '9a1ebff0eb181a222aaeec684f9d8b68ec6d45f96af399dadb118e4089a966a2@integration.gehealthcare.com'
               }
        print(sessionsurl)
        sessionsdata = requests.get(sessionsurl, headers=hdr)
        if sessionsdata.status_code == 200 and len(sessionsdata.json()['data']) != 0:
            usersessions(sessionsdata)

#################################################
[root@ip-10-242-109-196 /]# cat /home/hvr/smartsheets_prod_US131323/smartsheet_prod.py
import requests,json
import pandas as pd
from sqlalchemy import create_engine,types
import configparser
import psycopg2
import sqlalchemy as sa
config_path = '/home/hvr/.aws/redshift_connection.ini'
connection_profile = 'Connections_PROD'
# Pick from the following
# ['Connections_PROD', 'Connections_INNOVATION', 'Connections_FINNPROD', 'Connections_FINPROD']
def read_config_file(filepath, connection):
    config = configparser.ConfigParser()
    config.read(filepath)
    db_name = config[connection]['dbname']
    user = config[connection]['user']
    password = config[connection]['password']
    host = config[connection]['host']
    port = int(config[connection]['port'])
    return db_name, user, password, host, port


DBNAME, USER, PASS, HOST, PORT = read_config_file(config_path, connection_profile)
conn = psycopg2.connect(dbname=DBNAME, user=USER, password=PASS, host=HOST, port=PORT)
engine = sa.create_engine(f"postgresql://{USER}:{PASS}@{HOST}:{PORT}/{DBNAME}", echo=True).connect().execution_options(autocommit=True)

id_lst = ["382937121220484"]
table_lst = ["FRC_SMARTSHEET_GLOBAL_EXEC_REV_COM_S"]

#id_lst = ["5502354813413252","382937121220484", "1285453648291716", "3304683113604996", "1449940292528004", "3474967863027588", "2632588511733636"]
#table_lst = ["FRC_SMARTSHEET_CFO_CALL_SUM_NOTES_S", "FRC_SMARTSHEET_GLOBAL_EXEC_REV_COM_S", "FRC_SMARTSHEET_MAKE_CENTER_ROLE_S", "FRC_SMARTSHEET_MODALITY_EXEC_REV_COM_S", "FRC_SMARTSHEET_PLANT_COMMIT_S", "FRC_SMARTSHEET_FINANCE_ESTIMATE_DATE_S", "FRC_SMARTSHEET_FG_OPTION_ENABLED_S"]


token = "2GWV1KIIxRjwDfGC2T413sg1YtMo4KKOtzyeU"
proxy = { "https": "https://http-proxy.health.ge.com:88"}
hedrs = { "Authorization" : f"Bearer {token}", "Accept": "text/csv"}

#def sqlcol(dfparam):
#    dtypedict = {}
#    for i, j in zip(dfparam.columns, dfparam.dtypes):
#        if "object"in str(j):
#           dtypedict.update({i: types.VARCHAR(collation='case_insensitive')})
#    return dtypedict

for id,tablename in zip(id_lst,table_lst):
    url = f"https://api.smartsheet.com/2.0/sheets/{id}"
    response = requests.get(url, headers=hedrs)
    response.raise_for_status()
    sheet = response.content
    sheetname = f"{id}"
    print(sheetname)
    with open(f"/home/hvr/smartsheets_prod_US131323/data/{sheetname}.csv", 'wb+') as f:
        f.write(sheet)
    # conn = psycopg2.connect(dbname="gehc_data", user="502835360", host="us-prod-etl-alpha-redshift.cjn5iynjwss7.us-east-1.redshift.amazonaws.com", port="5439", password="OMi7oM#TY#a")
    Q1= f"""truncate table "{sheetname}" """
    conn = psycopg2.connect(dbname=DBNAME, user=USER, password=PASS, host=HOST, port=PORT)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(Q1)

# engine = create_engine("postgresql://502835360:OMi7oM#TY#a@us-prod-etl-alpha-redshift.cjn5iynjwss7.us-east-1.redshift.amazonaws.com:5439/gehc_data",echo=True)
# conn = engine.connect()
# conn.autocommit = True

for id,tablename in zip(id_lst,table_lst):
    sheetname = f"{id}"
    print(sheetname)
    df = pd.read_csv(f"/home/hvr/smartsheets_prod_US131323/data/{sheetname}.csv",)
    df = df.dropna(how='all')
    df = df.where(pd.notnull(df), None)
    #print(df.columns,df.dtypes)
    #df.to_sql(sheetname,con=conn,if_exists='append',chunksize = 1000,index=False,dtype= sqlcol(df))
    df.to_sql(sheetname,con=engine,if_exists='append',chunksize = 1000,index=False)

for id,tablename in zip(id_lst,table_lst):
    # conn = psycopg2.connect(dbname="gehc_data", user="502835360", host="us-prod-etl-alpha-redshift.cjn5iynjwss7.us-east-1.redshift.amazonaws.com", port="5439", password="OMi7oM#TY#a")
    Q1=f"""truncate table isc_smartsheet_fr."{tablename}" """
    Q2= f"""insert into  isc_smartsheet_fr."{tablename}"  (select * from "{id}") """
    conn = psycopg2.connect(dbname=DBNAME, user=USER, password=PASS, host=HOST, port=PORT)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(Q1)
    cursor.execute(Q2)
#####################
import requests
import numpy as np
import pandas as pd
import json
import logging
import os
import datetime
import config
import sqlalchemy as sa
import sys
from time import sleep
import warnings
from cryptography.utils import CryptographyDeprecationWarning

warnings.filterwarnings("ignore", category=CryptographyDeprecationWarning)

# tables = ['kbb_cards', 'kbb_moves', 'kbb_loops', 'kbb_parts', 'kbb_status_history']
table = sys.argv[1]

# Arguments follow the format:
# [table]
# Example:
# python3 full_load.py kbb_loops


class LOGGER_CLASS:
    '''
    Logs info to stream and file
    '''
    def __init__(self):
        self._log_format = f"%(asctime)s - [%(levelname)s] - %(name)s - (%(filename)s).%(funcName)s(%(lineno)d) - %(message)s"
        self.today = datetime.datetime.today()


    def get_file_handler(self):
        if not os.path.exists(config.script_location + os.sep + 'logs'):
            os.mkdir(config.script_location + os.sep + 'logs')
        file_handler = logging.FileHandler(config.script_location + os.sep + 'logs' + os.sep + f"ingestion_logs_kanbantest_{self.today.strftime('%Y-%m-%d')}.log")
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(logging.Formatter(self._log_format, datefmt='%Y/%m/%d %H:%M:%S'))
        return file_handler


    def get_stream_handler(self):
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(logging.Formatter(self._log_format, datefmt='%Y/%m/%d %H:%M:%S'))
        return stream_handler


    def get_logger(self, name):
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        logger.addHandler(self.get_file_handler())
        logger.addHandler(self.get_stream_handler())
        return logger

class SourceToFile:
    '''
    All functions to paginate the API response from source to a csv file.
    '''
    def __init__(self, logger, keys, filename):
        self.logger = logger
        self.keys = keys
        self.filename = filename
        self.table = table
        self.branch = config.tables[table]['branch']
        self.url = 'https://api.kanbanbox.com/rest/v0/' + config.tables[table]['url_suffix']
        self.params = config.tables[table]['params']
        self.proxies = config.proxies
        self.append = False


    def request_loop(self, headers, count, key):
        for i in range(count//self.params['limit']+1): # Pagination loop
            for tries in range(3):
                try:
                    response = requests.request("GET", self.url, headers=headers, params=self.params, proxies=self.proxies, allow_redirects=False)
                    res_list = json.loads(response.text)[self.branch]
                    if res_list == []:
                        self.logger.info(f'Empty list found where records were expected for {key}')
                        raise Exception(f'Empty list found where records were expected for {key}')
                    temp = pd.DataFrame(res_list)
                    temp['org'] = self.keys[self.keys['API_KEY']==key]['ORG'].iloc[0]
                    temp.replace({None: np.nan}, inplace=True)
                    temp.to_csv(config.script_location+os.sep+'data'+os.sep+self.filename, mode='a', index=False, header=not self.append)
                    # temp.to_parquet(config.script_location+os.sep+'data'+os.sep+self.filename, engine='fastparquet', index=False, append=self.append)
                    self.append = True
                    if i != count//self.params['limit']:
                        self.logger.info(f"Loaded {json.loads(response.text)['metadata']['limit']} for {key}")
                    else:
                        self.logger.info(f"Loaded {int(json.loads(response.text)['metadata']['totalCount'])%self.params['limit']} for {key}")
                    break
                except Exception as e:
                    if tries == 2:
                        self.logger.info(f'Exception {e} was made. Moving to the next entry')
                        self.logger.info(requests.request("GET", self.url, headers=headers, params=self.params, proxies=self.proxies, allow_redirects=False).json()['metadata'])
                        raise Exception(f'Unable to get count:\n{str(e)[:100]}')
                    self.logger.info(f'Exception created at try number: {tries+1}')
                    sleep(10)
            self.params['offset'] = self.params['offset'] + self.params['limit']


    def check_count(self, headers):
        for tries in range(3):
            try:
                count_params = self.params.copy()
                count_params['limit'] = 1
                response = requests.request("GET", self.url, headers=headers, params=count_params, proxies=self.proxies, allow_redirects=False)
                count = json.loads(response.text)['metadata']['totalCount']
                return count
            except Exception as e:
                if tries == 2:
                    self.logger.info(f'Exception {e} was made while getting count. Moving to the next entry')
                    self.logger.info(requests.request("GET", self.url, headers=headers, params=self.params, proxies=self.proxies, allow_redirects=False).text)
                    continue
                self.logger.info(f'Exception created at try number: {tries+1}')
                sleep(10)


    def write_to_file(self):
        '''
        Gets the count from source, then paginates then runs the request loop that writes the paginated response to a csv file.
        '''
        if os.path.exists(config.script_location + os.sep + 'data' + os.sep + self.filename): os.remove(config.script_location + os.sep + 'data' + os.sep + self.filename)
        self.append = False
        for key in keys['API_KEY']:
            self.params['offset'] = 0
            headers = {
            'Authorization': f'Bearer {key}',
            'Content-Type': 'text/plain'
            }
            count = self.check_count(headers)
            if count:
                self.request_loop(headers, count, key)
            else:
                self.logger.info(f'Key {key} skipped due to empty response. Moving to next key')
        self.logger.info('Finished writing to csv. Moving data to Redshift.')



def typecast_df(df, table):
    typecast = config.tables[table]['typecast']
    if 'integer' in typecast.keys():
        for col in typecast['integer']:
            df[col] = pd.to_numeric(df[col], errors='coerce', downcast='integer')
    if 'float' in typecast.keys():
        for col in typecast['float']:
            df[col] = pd.to_numeric(df[col], errors='coerce', downcast='float')
    if 'bool' in typecast.keys():
        for col in typecast['bool']:
            df[col] = df[col].map({"1": True, "0": False})
    return df


def write_to_database(logger, filename, engine):
    '''
    Reads df from the csv file and ingests into Redshift with the configured datatypes
    '''
    df = pd.read_csv(config.script_location+os.sep+'data'+os.sep+filename, index_col=False, low_memory=False)
    # df = pd.read_parquet(config.script_location+os.sep+'data'+os.sep+filename, engine='fastparquet')
    if df.empty:
        logger.info('Empty file found. Ending ingestion')
        return None
    logger.info('Reading from csv file: ')
    logger.info(df.head())
    if sa.inspect(engine).has_table(table, schema=config.target_schema):
        engine.execute(f"TRUNCATE TABLE {config.target_schema}.{table}")
        logger.info(f"Truncated table {config.target_schema}.{table}")
    df = typecast_df(df, table)
    df['ingestion_timestamp'] = datetime.datetime.now()
    df.to_sql(
        table, schema=config.target_schema, con=engine, if_exists='append',
        chunksize=10000, method='multi',
        index=False, dtype=config.tables[table]['datatypes']
        )
    os.remove(config.script_location+os.sep+'data'+os.sep+filename)
    logger.info(f'Moved data to Redshift successfully with the shape: {df.shape}')


if __name__ == '__main__':
    logger = LOGGER_CLASS().get_logger(__name__)
    if not os.path.exists(config.script_location + os.sep + 'data'):
        os.mkdir(config.script_location + os.sep + 'data')
    keys = pd.read_csv(config.script_location + os.sep + 'keys.csv', index_col=False)
    filename = f"{table}_{datetime.datetime.now().date()}.csv"
    write = SourceToFile(logger, keys, filename)
    write.write_to_file()
    file_exists = os.path.exists(config.script_location + os.sep + 'data' + os.sep + filename)
    engine = sa.create_engine(f"postgresql://{config.USER}:{config.PASS}@{config.HOST}:{config.PORT}/{config.DBNAME}", echo=False).connect().execution_options(autocommit=True)
    try:
        write_to_database(logger, filename, engine)
    except Exception as e:
        if file_exists:
            os.system(f'echo Could not write to Redshift :{config.target_schema}.{table} in {config.connection_profile}. Response file exists. {e} | mailx -s "{config.target_schema}.{table} file was not sent to Redshift" {config.email_address}')
        else:
            os.system(f'echo Could not write to Redshift :{config.target_schema}.{table} in {config.connection_profile} due to empty response. {e} | mailx -s "{config.target_schema}.{table} file was not sent to Redshift" {config.email_address}')
#######################################3

import requests
import json
import time
import os
import shutil

ts = time.time()
stamp = int(ts)

filepath = "/home/hvr/apex/apex_data_fullload/"

LST = ["USER", "USERROLES", "USERSSOROLES", "ADDRESS", "APPROVER", "BANKING", "CERTIFICATION", "COMPANYCODE", "DETAIL", "ERPMAPPING", "GENERIC", "IDENTITY", "PERSON", "PHONE", "TAX", "VENDOR", "EMPLOYEE", "DROPDOWNVALUE", "DROPDOWNGROUP"]

fail_obj = []
for i in LST:
    url = f"https://gehc.apexportal.net/apexportal.Odata.Service/odata/{i}?%20$select=*&$top=99999999"
    payload={}
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Basic T2RhdGFVc2VyOkAkb2FyQXBleCEyMDIz'   # new credentials :  user=OdataUser, pwd=@$oarApex!2023
    }
    response = requests.request("GET", url, headers=headers, data=payload)
    final_df = response.text
    if (response.status_code == 200):
        cwd = os.getcwd()
        rmpath = cwd + f"/apex_data_fullload/"
        path = os.path.join(rmpath, i)
        shutil.rmtree(path)
        print(i)
        os.makedirs(filepath+i, mode=0o777)
        with open(cwd + f"/apex_data_fullload/{i}/{i}_{stamp}.json", "w") as fl:
            fl.write(final_df)
    else:
        print("Failure Object : ", i)
        fail_obj.append(i)
################################


[hvr@ip-10-242-109-196 ~]$ cat /home/hvr/hawkeye_prod/cloud_opal.py
import os
import json
import requests
from datetime import datetime
import numpy as np
import urllib3
import pandas as pd
import psycopg2
import glob
import psycopg2.extras as extras
from sqlalchemy import create_engine
import configparser
import sqlalchemy as sa
config_path = '/home/hvr/.aws/redshift_connection.ini'
connection_profile = 'Connections_PROD'
# Pick from the following
# ['Connections_PROD', 'Connections_INNOVATION', 'Connections_FINNPROD', 'Connections_FINPROD']
def read_config_file(filepath, connection):
    config = configparser.ConfigParser()
    config.read(filepath)
    db_name = config[connection]['dbname']
    user = config[connection]['user']
    password = config[connection]['password']
    host = config[connection]['host']
    port = int(config[connection]['port'])
    return db_name, user, password, host, port

DBNAME, USER, PASS, HOST, PORT = read_config_file(config_path, connection_profile)

conn = psycopg2.connect(dbname=DBNAME, user=USER, password=PASS, host=HOST, port=PORT)
conn.autocommit = True

filePath = '/home/hvr/hawkeye_prod/data/'

showpadfiles = glob.glob(filePath + '*')
for f in showpadfiles:
    os.remove(f)

extDate = datetime.now().strftime('%d-%m-%Y')
logs = []
reslt = []


def get_auth_headers():
    request_body = {
        "client_id": 'HC-Asset',
        "client_secret": '0e0065acd1e1b91e5922a07979d78bbbd12b95af',
        "scope": "api",
        "grant_type": "client_credentials",
    }

    token = requests.post(
        "https://fssfed.ge.com/fss/as/token.oauth2",
        data=request_body,
        headers={"Accept": "application/json"},
        verify=False,
    ).json()

    headers = {
        "Authorization": "{} {}".format(token["token_type"], token["access_token"]),
        "x-api-key": "LHnZRBtyRA1jFyS7YdjqE9rNE4g0st2G1lKIrTVj"
    }

    return headers


def search_request(body):
    global akana_headers
    res = requests.post(
        search_url, headers=akana_headers, verify=False, data=json.dumps(body)
    )
    if res.status_code == requests.codes.unauthorized:
        akana_headers = get_auth_headers()
        res = requests.post(
            search_url, headers=akana_headers, verify=False, data=json.dumps(body)
        )
    print(res)
    return res


cols = ['model' ,'model_manufacturer' ,'model_classification' ,'u_unique_app_id' ,'u_config_item_id' ,'u_business_unit' ,'sys_class_name' ,'name' ,'managed_department' ,'department' ,'u_business_segment' ,'company' ,'service_classification' ,'vendor' ,'change_control' ,'owner_name' ,'owner_sso' ,'operational_status' ,'install_status' ,'install_date' ,'source_table' ,'sys_updated_on' ,'es_insert_time' ,'support_group_email' ,'support_group' ,'hvr_last_upd_tms']


def fetchData(app_results):
    df = pd.DataFrame(
        {'model' :[],'model_manufacturer' :[],'model_classification' :[],'u_unique_app_id' :[],'u_config_item_id' :[],'u_business_unit' :[],'sys_class_name' :[],'name' :[],'managed_department' :[],'department' :[],'u_business_segment' :[],'company' :[],'service_classification' :[],'vendor' :[],'change_control' :[],'owner_name' :[],'owner_sso' :[],'operational_status' :[],'install_status' :[],'install_date' :[],'source_table' :[],'sys_updated_on' :[],'es_insert_time' :[],'support_group_email' :[],'support_group' :[],'hvr_last_upd_tms' :[]})
    for item in app_results:
        df = df.append(
            {'model':item.get('model') ,'model_manufacturer':item.get('model_manufacturer') ,'model_classification':item.get('model_classification') ,'u_unique_app_id':item.get('u_unique_app_id') ,'u_config_item_id':item.get('u_config_item_id') ,'u_business_unit':item.get('u_business_unit') ,'sys_class_name':item.get('sys_class_name') ,'name':item.get('name') ,'managed_department':item.get('managed_department') ,'department':item.get('department') ,'u_business_segment':item.get('u_business_segment') ,'company':item.get('company') ,'service_classification':item.get('service_classification') ,'vendor':item.get('vendor') ,'change_control':item.get('change_control') ,'owner_name':item.get('owner_name') ,'owner_sso':item.get('owner_sso') ,'operational_status':item.get('operational_status') ,'install_status':item.get('install_status') ,'install_date':item.get('install_date') ,'source_table':item.get('source_table') ,'sys_updated_on':item.get('sys_updated_on') ,'es_insert_time':item.get('es_insert_time') ,'support_group_email':item.get('support_group_email') ,'support_group':item.get('support_group') ,'hvr_last_upd_tms':item.get('hvr_last_upd_tms') }, ignore_index=True)
    return df

def get_apps():
    body =  {
        "size": 9999,
        "query": {
            "bool": {
                "must": [
                    {"match": {"sys_class_name": "cmdb_ci_service"}},
                    {"match": {"model": "AMAZON WEB SERVICES (AWS) VPC"}},
                    {"match": {"company": "GE Healthcare"}}
                ]
            }
        }
    }
    try:
        res_json = search_request(body).json()
        data = res_json['hits']['hits']
        app_results = [a["_source"] for a in data]
        reslt = fetchData(app_results)
        totalcount = len(reslt)
        print('Count is :' + str(totalcount))
    except KeyError:
        data = []
        #_scroll_id = _scroll_id
    # while data:
    #     bodyScroll = {
    #         "scroll": "20m",
    #         "_scroll_id": _scroll_id
    #     }
    #     scroll_body = json.dumps(bodyScroll)
    #     akana_headers = get_auth_headers()
    #     scroll_res = requests.post(
    #         search_url, headers=akana_headers, verify=False, data=scroll_body
    #     )
    #     print(scroll_res)
    #     try:
    #         res_json = scroll_res.json()
    #         data = res_json['hits']['hits']
    #         _scroll_id = res_json['_scroll_id']
    #         app_results = [a["_source"] for a in data]
    #         reslt = reslt.append(fetchData(app_results))
    #         totalcount = len(reslt)
    #         print('Count is :' + str(totalcount))
    #     except KeyError:
    #         _scroll_id = _scroll_id
    #         print('Error: Elastic Search Scroll: %s' % str(scroll_res.json()))
    print('res')
    df1 = pd.DataFrame(reslt, columns=cols)
    df1.to_csv(filePath + 'cloudopalconfig.csv', index=False)


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


if __name__ == "__main__":
    try:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        akana_headers = get_auth_headers()
        search_url = "https://api.ge.com/digital/opal/v3/assets/elastic/search"
        start = datetime.now()
        get_apps()
        df = pd.read_csv(filePath + 'cloudopalconfig.csv',low_memory=False)
        df = df.where(pd.notnull(df), None)
        df['hvr_last_upd_tms'] = datetime.now()
        print(df.shape)
        conn.autocommit = True
        Q1 = f"""truncate table opal_fr.cloud_opal"""
        cursor = conn.cursor()
        cursor.execute(Q1)
        execute_values(conn, df, 'opal_fr.cloud_opal')
        print("success")
    except Exception as e:
        print(e)
############################################

import os
import json
import requests
from datetime import datetime
import numpy as np
import urllib3
import pandas as pd
import psycopg2
import glob
import psycopg2.extras as extras
from sqlalchemy import create_engine
import configparser
import sqlalchemy as sa
config_path = '/home/hvr/.aws/redshift_connection.ini'
connection_profile = 'Connections_PROD'
# Pick from the following
# ['Connections_PROD', 'Connections_INNOVATION', 'Connections_FINNPROD', 'Connections_FINPROD']
def read_config_file(filepath, connection):
    config = configparser.ConfigParser()
    config.read(filepath)
    db_name = config[connection]['dbname']
    user = config[connection]['user']
    password = config[connection]['password']
    host = config[connection]['host']
    port = int(config[connection]['port'])
    return db_name, user, password, host, port

DBNAME, USER, PASS, HOST, PORT = read_config_file(config_path, connection_profile)


conn = sa.create_engine(f"postgresql://{USER}:{PASS}@{HOST}:{PORT}/{DBNAME}").connect().execution_options(autocommit=True)
conn = psycopg2.connect(dbname=DBNAME, user=USER, password=PASS, host=HOST, port=PORT)
conn.autocommit = True

filePath = '/home/hvr/hawkeye_prod/data/'

showpadfiles = glob.glob(filePath + '*')
for f in showpadfiles:
    os.remove(f)

extDate = datetime.now().strftime('%d-%m-%Y')
logs = []
reslt = []


def get_auth_headers():
    request_body = {
        "client_id": 'HC-Asset',
        "client_secret": '0e0065acd1e1b91e5922a07979d78bbbd12b95af',
        "scope": "api",
        "grant_type": "client_credentials",
    }

    token = requests.post(
        "https://fssfed.ge.com/fss/as/token.oauth2",
        data=request_body,
        headers={"Accept": "application/json"},
        verify=False,
    ).json()

    headers = {
        "Authorization": "{} {}".format(token["token_type"], token["access_token"]),
        "x-api-key": "LHnZRBtyRA1jFyS7YdjqE9rNE4g0st2G1lKIrTVj"
    }

    return headers


def search_request(body):
    global akana_headers
    res = requests.post(
        search_url, headers=akana_headers, verify=False, data=json.dumps(body)
    )
    if res.status_code == requests.codes.unauthorized:
        akana_headers = get_auth_headers()
        res = requests.post(
            search_url, headers=akana_headers, verify=False, data=json.dumps(body)
        )
    print(res)
    return res


cols = ['u_config_item_id', 'name', 'infrasite', 'hvr_last_upd_tms']


cols=['sys_class_name','u_config_item_id','install_date','install_status','model','model_classification','name','owner_name','owner_sso','u_port_speed','scheduled_retirement']

def fetchData(app_results):
    df = pd.DataFrame({'sys_class_name':[],'u_config_item_id': [], 'install_date': [],'install_status': [],'model':[],'model_classification':[],'name':[],'owner_name':[],'owner_sso':[],'u_port_speed':[],'scheduled_retirement':[]
 })

    for item in app_results:
        s = {'sys_class_name': item.get('sys_class_name'),'u_config_item_id': item.get('u_config_item_id'), 'install_date': item.get('install_date'), 'install_status': item.get('install_status'),'model':item.get('model'),'model_classification':item.get('model_classification'),'name':item.get('name'),'owner_name':item.get('owner_name'),'owner_sso':item.get('owner_sso'),'u_port_speed':item.get('u_port_speed'),'scheduled_retirement':item.get('scheduled_retirement')
        }
        df= pd.concat([df,pd.DataFrame([s])],ignore_index=True)
    return(df)

def get_apps():
    body = {
  "scroll": "10m",
  "size": "9999",

    "_source": [
        "sys_class_name",
        "u_config_item_id",
        "install_date",
        "install_status","model","model_classification","name","owner_name","owner_sso",'scheduled_retirement','u_port_speed'
    ],
    "query": {
        "bool": {
            "should": [

                { "match": { "sys_class_name": "cmdb_ci_network_circuit" }},
                { "match": { "sys_class_name": "cmdb_ci_lb" }},
                { "match": { "sys_class_name": "cmdb_ci_display" }},
                { "match": { "sys_class_name": "cmdb_ci_ip_firewall" }},
                { "match": { "sys_class_name": "cmdb_ci_netgear" }},
                { "match": { "sys_class_name": "service_offering" }},

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
        totalcount = len(reslt)
        print('Count is :' + str(totalcount))
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
            search_url, headers=akana_headers, verify=False, data=scroll_body
        )
        print(scroll_res)
        try:
            res_json = scroll_res.json()
            data = res_json['hits']['hits']
            _scroll_id = res_json['_scroll_id']
            app_results = [a["_source"] for a in data]
            reslt = reslt.append(fetchData(app_results))
            totalcount = len(reslt)
            print('Count is :' + str(totalcount))
        except KeyError:
            _scroll_id = _scroll_id
            print('Error: Elastic Search Scroll: %s' % str(scroll_res.json()))
    print('res')
    df1 = pd.DataFrame(reslt, columns=cols)
    df1.to_csv(filePath + 'infrasiteopalconfig.csv', index=False)


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


if __name__ == "__main__":
    try:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        akana_headers = get_auth_headers()
        search_url = "https://api.ge.com/digital/opal/v3/assets/elastic/search"
        start = datetime.now()
        get_apps()
        df = pd.read_csv(filePath + 'infrasiteopalconfig.csv',low_memory=False)
        df = df.where(pd.notnull(df), None)
        df['hvr_last_upd_tms'] = datetime.now()
        print(df.shape)
        conn.autocommit = True
        Q1 = f"""truncate table opal_fr.infrasite_opal"""
        cursor = conn.cursor()
        cursor.execute(Q1)
        execute_values(conn, df, 'opal_fr.infrasite_opal')
        print("success")
    except Exception as e:
        print(e)

########################

import argparse
import requests
import os
import json
import configparser
from datetime import datetime
from pytz import utc
import numpy as np
import pandas as pd
from sqlalchemy import types, create_engine, inspect
from time import sleep
import logging
from urllib3 import disable_warnings, exceptions
disable_warnings(exceptions.InsecureRequestWarning)

class LOGGER_CLASS:
    '''
    Logs info to stream and file
    '''
    def __init__(self, log_location):
        self._log_format = f"%(asctime)s - [%(levelname)s] - %(name)s - (%(filename)s).%(funcName)s(%(lineno)d) - %(message)s"
        self.today = datetime.today()
        self.ingestion_name = 'pdx_hybris'
        self.log_location = log_location


    def get_file_handler(self):
        if not os.path.exists(self.log_location):
            os.mkdir(self.log_location)
        file_handler = logging.FileHandler(os.path.join(self.log_location, f"ingestion_logs_{self.ingestion_name}_{self.today.strftime('%Y-%m-%d')}.log"))
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(logging.Formatter(self._log_format, datefmt='%Y/%m/%d %H:%M:%S'))
        return file_handler


    def get_stream_handler(self):
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(logging.Formatter(self._log_format, datefmt='%Y/%m/%d %H:%M:%S'))
        return stream_handler


    def get_logger(self, name):
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        logger.addHandler(self.get_file_handler())
        logger.addHandler(self.get_stream_handler())
        return logger


def read_config_file(filepath, connection):
    config = configparser.ConfigParser()
    config.read(filepath)
    db_name = config[connection]['dbname']
    user = config[connection]['user']
    password = config[connection]['password']
    host = config[connection]['host']
    port = int(config[connection]['port'])
    return db_name, user, password, host, port


class SourceToDataframe:
    def __init__(self, parsed_json, connection_string, logger) -> None:
        self.parsed_json = parsed_json
        self.connection_string = connection_string
        self.username, self.password, self.schema, self.incremental, self.order_key, self.api_retry_count, self.email_address, self.tables, self.numeric_cols, self.char_cols, self.date_cols = self.get_variables(parsed_json)
        self.now = datetime.now(tz=utc)
        self.logger = logger

    def validate_datatypes(self, df, datatypes, table):
        for col in self.numeric_cols[table]:
            df[col] = df[col].replace({None: np.nan})
            df[col] = pd.to_numeric(df[col], errors='coerce')
        for col in self.date_cols[table]:
            df[col] = pd.to_datetime(df[col], errors='coerce', infer_datetime_format=True)
        for col in self.char_cols[table]:
            df[col] = df[col].replace({np.nan: ""})
            if isinstance(datatypes[col], types.VARCHAR):
                df[col] = df[col].astype(str)
            elif isinstance(datatypes[col], types.CHAR):
                df[col] = df[col].astype(str)
                df[col] = df[col].apply(lambda x: None if len(x) > 1 else x)
            df[col] = df[col].replace({"": None})
        return df

    def transform_datatypes(self, tables):
        numeric_cols = dict()
        date_cols = dict()
        char_cols = dict()
        for table, datatypes in tables.items():
            numeric_cols[table] = []
            date_cols[table] = []
            char_cols[table] = []
            for col, datatype in datatypes['datatypes'].items():
                datatype = datatype.lower()
                if 'varchar' in datatype:
                    tables[table]['datatypes'][col] = types.VARCHAR(int(datatype.split('(')[1].split(')')[0]), collation='case_insensitive')
                    char_cols[table].append(col.lower())
                elif 'timestamp' in datatype:
                    tables[table]['datatypes'][col] = types.TIMESTAMP()
                    date_cols[table].append(col.lower())
                elif 'char' in datatype:
                    tables[table]['datatypes'][col] = types.CHAR(collation='case_insensitive')
                    char_cols[table].append(col.lower())
                elif 'date' in datatype:
                    tables[table]['datatypes'][col] = types.DATE()
                    date_cols[table].append(col.lower())
                elif 'decimal' in datatype:
                    precision = int(datatype.split('(')[1].split(')')[0].split(',')[0].strip())
                    scale = None
                    if "," in datatype.split('(')[1]:
                        scale = int(datatype.split('(')[1].split(')')[0].split(',')[1].strip())
                    tables[table]['datatypes'][col] = types.DECIMAL(precision, scale)
                    numeric_cols[table].append(col.lower())
                elif 'float' in datatype:
                    if "," in datatype.split('(')[1]:
                        precision = int(datatype.split('(')[1].split(')')[0].split(',')[0].strip())
                        if "," in datatype.split('(')[1]:
                            scale = int(datatype.split('(')[1].split(')')[0].split(',')[1].strip())
                            tables[table]['datatypes'][col] = types.NUMERIC(precision, scale)
                        else:
                            tables[table]['datatypes'][col] = types.FLOAT(precision)
                    numeric_cols[table].append(col.lower())
                elif 'real' in datatype:
                    if "," in datatype.split('(')[1]:
                        precision = int(datatype.split('(')[1].split(')')[0])
                        tables[table]['datatypes'][col] = types.REAL(precision=precision)
                    numeric_cols[table].append(col.lower())
                elif 'integer' in datatype:
                    if int(datatype.split('(')[1].split(')')[0]) > 9:
                        tables[table]['datatypes'][col] = types.BIGINT()
                    else:
                        tables[table]['datatypes'][col] = types.INTEGER()
                    numeric_cols[table].append(col.lower())
        return tables, numeric_cols, char_cols, date_cols

    def get_variables(self, parsed_json):
        username = parsed_json['source']['api']['username']
        password = parsed_json['source']['api']['password']
        schema = parsed_json['target']['redshift']['schema']
        incremental = parsed_json['target']['redshift']['incremental']
        order_key = parsed_json['target']['redshift']['order_key']
        api_retry_count = parsed_json['target']['redshift']['api_retry_count']
        email_address = parsed_json['email_address']
        tables, numeric_cols, char_cols, date_cols = self.transform_datatypes({table.lower(): table_info for table, table_info in parsed_json['target']['redshift']['tables'].items()})
        return username, password, schema, incremental, order_key, api_retry_count, email_address, tables, numeric_cols, char_cols, date_cols

    def get_response(self, url):
        for tries in range(self.api_retry_count):
            try:
                sleep(1)
                self.logger.info(url)
                response = requests.request("GET", url, auth=(self.username, self.password), verify=False, allow_redirects=False)
                assert response.status_code == 200
                response = response.json()
                return response
            except Exception as e:
                if (tries + 1) == self.api_retry_count:
                    self.logger.info(f"Max tries reached, due to error: \n{e}")
                    os.system(f'echo Response not found for the endpoint: {url}\n Could not get response for Hybris: {e} | mailx -s "Response not found for the hybris endpoint" {self.email_address}')
                else:
                    self.logger.info(f"API Response failed at try: {tries + 1}")
        return None

    def traverse_response(self, response_walk, response):
        if response_walk:
            try:
                for i in response_walk:
                    response = response[i]
            except Exception as e:
                self.logger.info(f"Exception reached when walking through the response. Please check if the reponse contains {response_walk}\nResponse: {response.text}\n{e}")
        return response

    def get_df(self, table, datatypes, url, response_walk):
        response = self.get_response(url)
        if response:
            response = self.traverse_response(response_walk, response)
            if table == "rest_cdx_hybris_webordering_referring_physician":
                df = pd.json_normalize(response, "orderMaterial", ['customerId', 'orderCode', 'orderCreationTime', 'orderPurchaseOrderNumber', 'shipToCountry', 'shipToId', 'shipToName', 'shipToPostCode', 'shipToRegion', 'orderPhysicianName'], errors='ignore')
            else:
                df = pd.DataFrame(response)
            df.columns = [i.lower().strip() for i in df.columns]
            df[self.order_key] = self.now
            for col in datatypes.keys():
                if col not in df.columns:
                    df[col] = None
            df = df[[i.lower() for i in datatypes.keys()]]
            df = df.dropna(how='all')
            df = self.validate_datatypes(df, datatypes, table)
            return df
        return pd.DataFrame()

    def get_date_from_redshift(self, table):
        with create_engine(self.connection_string, executemany_mode='batch', pool_size=5, max_overflow=8).connect().execution_options(autocommit=True) as conn:
            result = conn.execute(f'SELECT MAX({self.order_key}) from {self.schema}.{table} limit 1;')
            result = result.first()[0]
        return result

    def get_table_variables(self, table, incremental):
        datatypes = {col.lower(): datatype for col, datatype in self.tables[table]['datatypes'].items()}
        if incremental:
            first_date = self.get_date_from_redshift(table)
            last_date = self.now
        else:
            first_date = datetime(2015,3,13,0,0,0, tzinfo=utc)
            last_date = self.now
            if table.lower() in ["rest_cdx_hybris_webordering_referring_physician"]:
                self.logger.info('perform full load for physician table in chunks')
                return -1
        url = self.parsed_json['source']['api']['apiurl'] + '/' + self.tables[table]['url_suffix'] + '/' + first_date.strftime('%Y-%m-%dT%H:%M:%S%z') + '/' + last_date.strftime('%Y-%m-%dT%H:%M:%S%z')
        response_walk = self.tables[table]['response_walk']
        datatypes[self.order_key] = types.TIMESTAMP(timezone=True)
        return datatypes, url, response_walk



class DataframeIngest:
    def __init__(self, df, datatypes, connection_string, schema, table, primary_keys, incremental, logger) -> None:
        self.df = df
        self.datatypes = datatypes
        self.engine = create_engine(connection_string)
        self.schema = schema
        self.table = table.lower()
        self.staging_table = table.lower() + '_stg'
        self.primary_keys = primary_keys
        self.incremental = incremental
        self.logger = logger

    def ingest_to_stg(self):
        '''
        Reads df and ingests into staging table with the configured datatypes
        '''
        self.logger.info(self.df.head())
        if inspect(self.engine).has_table(self.staging_table, schema=self.schema):
            self.engine.execute(f"TRUNCATE TABLE {self.schema}.{self.staging_table}")
            self.logger.info(f"Truncated table {self.schema}.{self.staging_table}")
        self.df.to_sql(
            self.staging_table, schema=self.schema, con=self.engine, if_exists='append',
            chunksize=20000, method='multi',
            index=False, dtype=self.datatypes
            )
        self.logger.info(f'Moved data to staging layer')

    def stg_to_final(self):
        '''
        Delete insert logic from staging to final redshift table
        '''
        if inspect(self.engine).has_table(self.table, schema=self.schema):
            query=f"DELETE FROM {self.schema}.{self.table} USING {self.schema}.{self.staging_table} WHERE"
            for i in self.primary_keys:
                sub_str=f"{self.schema}.{self.table}.{i.lower()} = {self.schema}.{self.staging_table}.{i.lower()} and"
                query = query+' '+sub_str
            list2 = query.split()[:-1]
            fin_query =' '.join(list2)+";"
            self.logger.info(fin_query)
            self.engine.execute(fin_query)
            self.engine.execute(f'INSERT INTO {self.schema}.{self.table} (SELECT * FROM {self.schema}.{self.staging_table});')
        else:
            self.engine.execute(f'CREATE TABLE {self.schema}.{self.table} AS (SELECT * FROM {self.schema}.{self.staging_table});')
            self.df.to_sql(
            self.table, schema=self.schema, con=self.engine, if_exists='append',
            chunksize=20000, method='multi',
            index=False, dtype=self.datatypes
            )
        self.logger.info(f'Moved data to Redshift successfully with the shape: {self.df.shape}')

    def main(self):
        self.ingest_to_stg()
        self.stg_to_final()


def main():
    parser = argparse.ArgumentParser(description='API to Redshift With Basic Auth')
    parser.add_argument('--infile', nargs=1, help="JSON file to be processed", type=argparse.FileType('r'))
    arguments = parser.parse_args()
    parsed_json = json.load(arguments.infile[0])
    logger = LOGGER_CLASS(parsed_json['log_location']).get_logger(__name__)
    try:
        DBNAME, USER, PASS, HOST, PORT = read_config_file(parsed_json['target']['redshift']['config_path'], parsed_json['target']['redshift']['connection_profile'])
        connection_string = f"postgresql://{USER}:{PASS}@{HOST}:{PORT}/{DBNAME}"
        source_obj = SourceToDataframe(parsed_json, connection_string, logger)
        incremental = parsed_json['target']['redshift']['incremental']
        for table in source_obj.tables.keys():
            datatypes, url, response_walk = source_obj.get_table_variables(table, incremental)
            df = source_obj.get_df(table, datatypes, url, response_walk)
            if df.empty:
                logger.info("Empty dataframe found or Response not processed. Moving to the next table")
                continue
            primary_keys = [i.lower() for i in source_obj.tables[table]['primary_keys']]
            ingest_obj = DataframeIngest(df, datatypes, connection_string, source_obj.schema, table, primary_keys, incremental, logger)
            ingest_obj.main()
            logger.info(f"Ingested table {table} with shape {df.shape} successfully")
    except Exception as e:
        logger.exception(e)


if __name__ == "__main__":
    exit(main())
#################################################
[hvr@ip-10-242-109-196 ~]$ cat /hvr/sftp/SrcFiles/cto/hawk_validation.py
import pandas as pd
from datetime import datetime
import os
from utils import setup_logger, send_email_notification
import re
def fields_replaced(main_cols,meta_cols):
    Fields_Replace=[x for x in meta_cols if x not in main_cols]
    return ','.join(Fields_Replace)

def df(lis):
    df=pd.DataFrame()
    for d in lis:
        df=pd.concat([df,pd.DataFrame.from_dict(d,orient='index').transpose()])
    return df

def report(missing='',replaced='',newfields='',filenname='',pattern='',prev_count='',reseq='',count='',seq='',validation=''):
    report={"FILE PATTERN":"",
        "FILE NAME":"",
        "FIELD MISSING":"",
        "NEW FIELD ADDED":"",
        "FIELDS REPLACED":"",
        "SEQUENCE MATCHING":"",
        "VALIDATION STATUS":"",
        "ROW COUNT":"",
        "BILL DATE CHECK":"",
        "TOTAL CHARGE":"",
        "OCTO PLUS CHECK":"",
        "UNIQUE COL CHECK":"",
        "UNIQUE P_U_CONFIG_ITEM_ID COUNT":"",
        "LEANIX:MULTIPLE PRODUCT APPL COUNT":"",
        "RE SEQUENCE FLAG":"",
        "CURRENT MONTH FILE":"",
        "PREVIOUS MONTH COUNT":"",
        "PREVIOUS MONTH CHARGE":""}
    report['FIELD MISSING'],report['FIELDS REPLACED'],report['NEW FIELD ADDED'],report['FILE NAME'],report['FILE PATTERN'],report['PREVIOUS MONTH COUNT'],report['RE SEQUENCE FLAG'],report['ROW COUNT'],report['SEQUENCE MATCHING'],report['VALIDATION STATUS']=missing,replaced,newfields,filenname,pattern,prev_count,reseq,count,seq,validation
    lis.append(report)

def validation_status(validations,newfields,missing,replaced):
    if newfields=='' and missing=='' and replaced=='':
        validations=''
    elif len(missing)>0:
        validations.append('COLUMNS MISSING')
    return validations

def newfieldsadded(cols,metacols):
    newcols=[]
    validation=[]
    for c in cols:
        if c.lower() not in [x.lower() for x in metacols]:newcols.append(c)
    for n in newcols:
        if list(cols).index(n)<=len(metacols)-1:
            validation.append('FIELDS ADDED IN BETWEEN')
        else:
            validation.append('COLUMNS ADDED')
    return validation,','.join(list(newcols))

def missing(cols,metacols):
    mis=[]
    for c in metacols:
        if c.lower() not in [x.lower() for x in cols]:mis.append(c)
    return mis

def previous_count(path,pattern):
    try:
        prev=pd.read_csv(path,na_filter=False)
        count=prev[prev['FILE PATTERN']==pattern]['ROW COUNT']
        if len(count)!=0:
            return int((count.transpose().values))
        else:
            return ''
    except Exception:
        return ''

def re_seq(cols,metacols):
    match=cols==metacols
    if match==True:
        return 'Y','N'
    elif match==False:
        return'N','Y'

def sequencing(metacols,df,cols):
    new_df=pd.DataFrame(columns=metacols)
    for col in metacols:
        for c in cols:
            if c.lower() == col.lower():
                new_df[col]=df[c]
    return new_df

def matching(meta_columns,current_df,filename,pattern):
    logger.info(f"Checking {filename}")
    current_df=pd.read_csv(current_df,encoding='latin1')
    current_df=current_df.reset_index(drop=True)
    cols=list(current_df.columns)
    print(cols)
    for c in cols:
        i=cols.index(c)
        c = re.sub(r"[^a-zA-Z\_. ]", "",c)
        cols[i]=c
    current_df.columns=pd.Series(cols)
    print(current_df.columns)
    count=current_df.shape[0]
    columns=list(current_df.columns)
    prev_count=previous_count(prevfile,pattern)
    seq,reseq=re_seq(columns,meta_columns)
    if seq=='Y' and reseq=='N':
        logger.info(f"{filename} has been validated")
        report(filenname=filename,pattern=pattern,reseq=reseq,seq=seq,validation='COLUMNS MATCHED',prev_count=prev_count,count=count)
    else:
        validations,newfields=newfieldsadded(columns,meta_columns)
        missing_cols=missing(columns,meta_columns)
        replaced=fields_replaced(columns,meta_columns)
        validation=validation_status(validations,newfields,missing_cols,replaced)
        validation=','.join(set(validation))
        seq,reseq=re_seq(columns,meta_columns)
        logger.info(f"Generating new resequenced file for {filename} at {parent_path}")
        newfile=sequencing(meta_columns,current_df,columns)
        print(newfile)
        newfile.to_csv(os.path.join(parent_path,f"{filename}"),index=False)
        logger.info(f"New Re-Sequenced {filename} placed at {parent_path}")
        logger.info(f"{filename} has been validated")
        report(','.join(missing_cols),replaced,newfields,filename,pattern,prev_count,reseq,count,seq,validation)

def pickfiles(filenames):
    names=[]
    for f in filenames:
        f=f.replace('.csv','')
        f=f.replace('*','')
        names.append(f)
    return names

def checkfiles(names):
    f_names={}
    files=os.listdir(parent_path)
    for n in names:
        for f in files:
            if n in f:
                if f.endswith('.csv'):
                    f_names[n]=f
    return f_names

def src_meta(meta_path,filepath):
    logger.info("Picking Up Metadata File")
    meta=pd.read_csv(meta_path)
    names_df=pd.DataFrame(meta[meta['Sl.No'].notnull()]['File name'])
    filenames=names_df["File name"]
    names=pickfiles(list(filenames))
    logger.info("Checking for Source Files")
    files=checkfiles(names)
    not_found=names-files.keys()
    for f in not_found:
        logger.info(f"No file found for {f}")
        report(pattern='*'+f+'*.csv',validation='FILE NOT FOUND',prev_count=previous_count(prevfile,f),reseq='',seq='',count='')
    print(files)
    for key,values in files.items():
        filename='*'+key+'*.csv'
        value=names_df[names_df['File name']==filename].index[0]
        indices=list(names_df.index)
        val=indices.index(value)+1
        if val<len(indices):
            meta_df=pd.DataFrame(meta.loc[value:indices[val]-1].dropna(how='all').drop('Sl.No',axis=1))
        elif val==len(indices):
            meta_df=pd.DataFrame(meta.loc[value:].dropna(how='all').drop('Sl.No',axis=1))
        cols=list(meta_df['Column name'])
        file_name=values
        pattern=filename
        matching(meta_columns=cols,current_df=os.path.join(parent_path,file_name),filename=file_name,pattern=pattern)

if __name__=='__main__':
    parent_path = os.path.dirname(os.path.abspath(__file__))
    log_filename = str(__file__).replace('py', 'log')
    logger = setup_logger(os.path.join(parent_path,log_filename))
    lis=[]
    print(parent_path)
    prevfile='{}_{}_{}.csv'.format(os.path.join(parent_path,"CTO_FILE_VALIDATION_REPORT"),datetime.now().month-1,datetime.now().year)
    try:
        src_meta(os.path.join(parent_path,"Metadata_SRCfiles.csv"),parent_path)
        logger.info(f"Generating report for {datetime.now().month}-{datetime.now().year} at {parent_path}")
        df(lis).to_csv('{}_{}_{}.csv'.format(os.path.join(parent_path,"CTO_FILE_VALIDATION_REPORT"),datetime.now().month,datetime.now().year),index=False)
        logger.info("Report Generated Successfully")
    except Exception as e:
        logger.info(f"Some error occured while executing {e}")
        send_email_notification(f"Exception occured {e} at {parent_path}", "Hawkeye File Valiadation Failure|Prod")
#########################################
[hvr@ip-10-242-109-196 ~]$ cat /data/custom_scripts/Servicenow/scripts/snowAPI_to_redshift.py
import configparser
import psycopg2
import sqlalchemy as sa
from datetime import datetime
import sys
import os
import json,csv
import requests
import numpy as np
import urllib3
import pandas as pd
import glob
import psycopg2.extras as extras
from sqlalchemy import create_engine
import logging
import time



def read_config_file(filepath, connection):
    config = configparser.ConfigParser()
    config.read(filepath)
    db_name = config[connection]['dbname']
    user = config[connection]['user']
    password = config[connection]['password']
    host = config[connection]['host']
    port = int(config[connection]['port'])
    return db_name, user, password, host, port


def get_auth_headers():
    request_body = {
        "client_id": 'HC-Asset',
        "client_secret": '0e0065acd1e1b91e5922a07979d78bbbd12b95af',
        "scope": "api",
        "grant_type": "client_credentials",
    }

    token = requests.post(
        "https://fssfed.ge.com/fss/as/token.oauth2",
        data=request_body,
        headers={"Accept": "application/json"},
        verify=False,
    ).json()

    headers = {
        "Authorization": "{} {}".format(token["token_type"], token["access_token"]),
        "x-api-key": "LHnZRBtyRA1jFyS7YdjqE9rNE4g0st2G1lKIrTVj"
    }

    return headers


def search_request(body):
    global akana_headers
    res = requests.post(
        search_url, headers=akana_headers, verify=False, data=json.dumps(body)
    )
    if res.status_code == requests.codes.unauthorized:
        akana_headers = get_auth_headers()
        res = requests.post(
            search_url, headers=akana_headers, verify=False, data=json.dumps(body)
        )
    print(res)
    return res

def locationDetais_fetchData(location_results):
    df=pd.DataFrame({
        "u_location_sys_id":[], "u_location_code":[],"country":[],"u_region":[]
    })
    for item in location_results:
        if item.get('sys_id'):
            df=df.append({"u_location_sys_id":item.get('sys_id'), "u_location_code":item.get('u_location_code'),"country":item.get('country'),"u_region":item.get('u_region')},ignore_index=True)

    return df


def get_location_details(location_id_list2):

    body1={
    "scroll": "20m",
    "size": "1500",
    "sort": ["_doc"],
    "query": {
        "bool": {
            "must": [
                {"match": {"sys_class_name": "location"}},
                {"terms": {"sys_id": location_id_list2}}
            ]
        }
    }
}


    try:
        res_json = search_request(body1).json()
        data = res_json['hits']['hits']
        _scroll_id = res_json['_scroll_id']
        location_results = [a["_source"] for a in data]
        reslt = locationDetais_fetchData(location_results)
        totalcount = len(reslt)
    except Exception as e:
        print(e)
    df3 = pd.DataFrame(reslt)
    return df3


def fetchData(app_results):

    df = pd.DataFrame(
        {'sys_class_name': [], 'u_service': [], 'u_service_sys_id': [], 'number': [], 'state': [], 'priority': [],
         'opened_at': [], 'closed_at': [], 'resolved_at': [], 'close_notes': [], 'close_code': [],
         'short_description': [], 'description': [], 'business_duration_seconds': [], 'calendar_duration_seconds': [],
         'attr_seconds': [], 'dttr_seconds': [], 'business_impact': [], 'assignment_group': [], 'assigned_to': [],
         'outage_owner': [], 'caused_by': [], 'opened_by': [], 'cause': [], 'resolved_by': [], 'closed_by': [],
         'category': [], 'company': [], 'company_sys_id': [], 'sys_id': [], 'problem_id': [], 'inc_sys_id': [],
         'cmdb_ci_sys_id': [], 'cmdb_ci': [], 'u_environment': [], 'u_environment_sys_id': [], 'urgency': [],
         'subcategory': [], 'hold_reason': [],'u_location_sys_id':[]})
    for item in app_results:
        if item.get('sys_class_name')=='incident' and item.get('company')=='GE Healthcare' and item.get('assignment_group')=='@Digital BFE Support Team' or item.get('assignment_group')=='@Digital DST Customer AgentX' or item.get('assignment_group')=='@HEALTH AgentX EMEA L1 Support' or item.get('assignment_group')=='@HEALTH Apttus EMEA HYPERCARE L1' or item.get('assignment_group')=='@HEALTH BFE EMEA L1 Support' or item.get('assignment_group')=='@HEALTH CFD EMEA HYPERCARE L1' or item.get('assignment_group')=='@HEALTH CIV EMEA L1 Support' or item.get('assignment_group')=='@HEALTH Connectivity EMEA HYPERCARE L1' or item.get('assignment_group')=='@HEALTH Contract EMEA HYPERCARE L1' or item.get('assignment_group')=='@HEALTH CX EMEA HYPERCARE L1' or item.get('assignment_group')=='@HEALTH DATA EMEA HYPERCARE L1' or item.get('assignment_group')=='@HEALTH Depot Repair EMEA Hypercare L1' or item.get('assignment_group')=='@HEALTH E2E Corrective EMEA HYPERCARE L1' or item.get('assignment_group')=='@HEALTH EMEAHypercaresupport' or item.get('assignment_group')=='@HEALTH FMI EMEA HYPERCARE L1' or item.get('assignment_group')=='@HEALTH FRANCE AgentX L1 Support' or item.get('assignment_group')=='@HEALTH FRANCE BFE L1 Support' or item.get('assignment_group')=='@HEALTH FRANCE FX L1 Support' or item.get('assignment_group')=='@HEALTH FRANCE ServiceBoard L1 Support' or item.get('assignment_group')=='@HEALTH FRANCE SMAX L1 Support' or item.get('assignment_group')=='@HEALTH FX EMEA L1 Support' or item.get('assignment_group')=='@HEALTH GAMS Fx IT Support Team' or item.get('assignment_group')=='@HEALTH IB & Install EMEA HYPERCARE L1' or item.get('assignment_group')=='@HEALTH L1 Channel Partners EMEA' or item.get('assignment_group')=='@HEALTH MUST & SIEBEL EMEA HYPERCARE L1' or item.get('assignment_group')=='@HEALTH Next Gen CRM Support' or item.get('assignment_group')=='@HEALTH ODS EMEA HYPERCARE L1' or item.get('assignment_group')=='@HEALTH OneServicesCloud DataMigration' or item.get('assignment_group')=='@HEALTH Parts EMEA Hypercare L1' or item.get('assignment_group')=='@HEALTH PM EMEA Hypercare L1' or item.get('assignment_group')=='@HEALTH PM/FMI/E2E EMEA Hypercare L1' or item.get('assignment_group')=='@HEALTH Remote EMEA HYPERCARE L1' or item.get('assignment_group')=='@HEALTH Reporting EMEA HYPERCARE L1' or item.get('assignment_group')=='@HEALTH RX EMEA L1 Support' or item.get('assignment_group')=='@HEALTH ServiceBoard EMEA L1 Support' or item.get('assignment_group')=='@HEALTH SMAX EMEA L1 Support' or item.get('assignment_group')=='@HEALTH SMax GE Data Team' or item.get('assignment_group')=='@HEALTH SMax GE Support' or item.get('assignment_group')=='@HEALTH SMax Service Board Support' or item.get('assignment_group')=='@HEALTH SMax Vendor Support' or item.get('assignment_group')=='@HEALTH South Africa AgentX L1 Support' or item.get('assignment_group')=='@HEALTH South Africa BFE L1 Support' or item.get('assignment_group')=='@HEALTH South Africa FX L1 Support' or item.get('assignment_group')=='@HEALTH South Africa ServiceBoard L1 Support' or item.get('assignment_group')=='@HEALTH South Africa SMAX L1 Support':
            df = df.append(
                {'sys_class_name': item.get('sys_class_name'), 'u_service': item.get('u_service'),
                 'u_service_sys_id': item.get('u_service_sys_id'), 'number': item.get('number'), 'state': item.get('state'),
                 'priority': item.get('priority'), 'opened_at': item.get('opened_at'), 'closed_at': item.get('closed_at'),
                 'resolved_at': item.get('resolved_at'), 'close_notes': item.get('close_notes'),
                 'close_code': item.get('close_code'), 'short_description': item.get('short_description'),
                 'description': item.get('description'), 'business_duration_seconds': item.get('business_duration_seconds'),
                 'calendar_duration_seconds': item.get('calendar_duration_seconds'), 'attr_seconds': item.get('attr_seconds'),
                 'dttr_seconds': item.get('dttr_seconds'), 'business_impact': item.get('business_impact'),
                 'assignment_group': item.get('assignment_group'), 'assigned_to': item.get('assigned_to'),
                 'outage_owner': item.get('outage_owner'), 'caused_by': item.get('caused_by'), 'opened_by': item.get('opened_by'),
                 'cause': item.get('cause'), 'resolved_by': item.get('resolved_by'), 'closed_by': item.get('closed_by'),
                 'category': item.get('category'), 'company': item.get('company'), 'company_sys_id': item.get('company_sys_id'),
                 'sys_id': item.get('sys_id'), 'problem_id': item.get('problem_id'), 'inc_sys_id': item.get('inc_sys_id'),
                 'cmdb_ci_sys_id': item.get('cmdb_ci_sys_id'), 'cmdb_ci': item.get('cmdb_ci'),
                 'u_environment': item.get('u_environment'), 'u_environment_sys_id': item.get('u_environment_sys_id'),
                 'urgency': item.get('urgency'), 'subcategory': item.get('subcategory'), 'hold_reason': item.get('hold_reason'),'u_location_sys_id':item.get('u_location_sys_id')}, ignore_index=True)


    return df


def get_apps():
    body = {
        "scroll": "20m",
        "size": "1500",
        "sort": ["_doc"],
        "query": {
            "bool": {
            "must": [{"match": {"sys_class_name": "incident"}},

                     {"match": {"assignment_group": "@Digital BFE Support Team', '@Digital DST Customer AgentX', '@HEALTH AgentX EMEA L1 Support', '@HEALTH Apttus EMEA HYPERCARE L1', '@HEALTH BFE EMEA L1 Support', '@HEALTH CFD EMEA HYPERCARE L1', '@HEALTH CIV EMEA L1 Support', '@HEALTH Connectivity EMEA HYPERCARE L1', '@HEALTH Contract EMEA HYPERCARE L1', '@HEALTH CX EMEA HYPERCARE L1', '@HEALTH DATA EMEA HYPERCARE L1', '@HEALTH Depot Repair EMEA Hypercare L1', '@HEALTH E2E Corrective EMEA HYPERCARE L1', '@HEALTH EMEAHypercaresupport', '@HEALTH FMI EMEA HYPERCARE L1', '@HEALTH FRANCE AgentX L1 Support', '@HEALTH FRANCE BFE L1 Support', '@HEALTH FRANCE FX L1 Support', '@HEALTH FRANCE ServiceBoard L1 Support', '@HEALTH FRANCE SMAX L1 Support', '@HEALTH FX EMEA L1 Support', '@HEALTH GAMS Fx IT Support Team', '@HEALTH IB & Install EMEA HYPERCARE L1', '@HEALTH L1 Channel Partners EMEA', '@HEALTH MUST & SIEBEL EMEA HYPERCARE L1', '@HEALTH Next Gen CRM Support', '@HEALTH ODS EMEA HYPERCARE L1', '@HEALTH OneServicesCloud DataMigration', '@HEALTH Parts EMEA Hypercare L1', '@HEALTH PM EMEA Hypercare L1', '@HEALTH PM/FMI/E2E EMEA Hypercare L1', '@HEALTH Remote EMEA HYPERCARE L1', '@HEALTH Reporting EMEA HYPERCARE L1', '@HEALTH RX EMEA L1 Support', '@HEALTH ServiceBoard EMEA L1 Support', '@HEALTH SMAX EMEA L1 Support', '@HEALTH SMax GE Data Team', '@HEALTH SMax GE Support', '@HEALTH SMax Service Board Support', '@HEALTH SMax Vendor Support', '@HEALTH South Africa AgentX L1 Support', '@HEALTH South Africa BFE L1 Support', '@HEALTH South Africa FX L1 Support', '@HEALTH South Africa ServiceBoard L1 Support', '@HEALTH South Africa SMAX L1 Support"}},
                     {"match": {"company": "GE Healthcare"}}]
        }
        }
    }


    try:

        res_json = search_request(body).json()
        data = res_json['hits']['hits']
        _scroll_id = res_json['_scroll_id']
        app_results = [a["_source"] for a in data]
        reslt = fetchData(app_results)
        totalcount = len(reslt)
        logger.info(f' : Count is : {str(totalcount)}')
    except KeyError:
        data = []
        _scroll_id = _scroll_id
    loop =1
    while data:
        bodyScroll = {
            "scroll": "20m",
            "_scroll_id": _scroll_id
        }
        scroll_body = json.dumps(bodyScroll)
        #akana_headers = get_auth_headers()
        scroll_res = requests.post(
            search_url, headers=akana_headers, verify=False, data=scroll_body
        )

        try:
            res_json = scroll_res.json()
            data = res_json['hits']['hits']
            _scroll_id = res_json['_scroll_id']
            #print(_scroll_id)
            app_results = [a["_source"] for a in data]

            reslt = reslt.append(fetchData(app_results))

            totalcount = len(reslt)
            #print('Count is :' + str(totalcount))
            logger.info(f' Count is : {str(totalcount)}')
            loop +=1
        except Exception as e:
            _scroll_id = _scroll_id
            logger.error(f'Exception due to this error :{e}')
    logger.info(f'Total no. of pages scrolled : {loop}')
    df1 = pd.DataFrame(reslt)
    location_id_list=df1['u_location_sys_id'].to_list()
    location_id_set=set(location_id_list)
    location_id_list2=list(location_id_set)
    while None in location_id_list2:
       location_id_list2.remove(None)
    values = len(location_id_list2)
    #print("count of location id's :" + str(values))
    logger.info(f'Total number of location ids fetched from Incident tag : {values}')
    chunk_size = 100
    df_loc=pd.DataFrame()
    for i in range(0,len(location_id_list2),chunk_size):
        chunk = location_id_list2[i:i+chunk_size]
        df2=get_location_details(chunk)
        df_loc=df_loc.append(df2,ignore_index=True)
    logger.info(f'Location details has been fetched from Location tag into a Dataframme')
    df4=pd.merge(df1, df_loc, on='u_location_sys_id', how='left' )
    df4['load_dtm']=datetime.now()
    #print(df4)
    logger.info(f'Location Dataframe merged with Incident Dataframe : {df4}')
    return df4

if __name__ == "__main__":
    try:
        current_datetime=datetime.now()
        print("Current run date and time:",current_datetime)
        arg=sys.argv[1]
        with open(arg) as userfile:
            content_file=userfile.read()
        parsed_json=json.loads(content_file)
        config_path = parsed_json['DB_config_path']
        connection_profile = parsed_json['DB_connection_profile']
        tablename = parsed_json['Table_Name']
        schemaname =  parsed_json['Schema_Name']
        dl = parsed_json['Email_info']['dl']
        logfile = parsed_json['logfilepath']
        subject = parsed_json['Email_info']['subject']
        Message_Ingestion = parsed_json ['Email_info']['Message_Ingestion']
        #######Logging###########
        with open(f'{logfile}', 'w', newline='') as csvfl:
            write_csv = csv.writer(csvfl, delimiter='|')
            write_csv.writerow(['\t TimeStamp \t', 'Severity', '\t Message \t'])
        logging.basicConfig(filename=logfile,level=logging.INFO, format=('%(asctime)s | %(levelname)s | %(message)s'))
        logger = logging.getLogger('hvr')
        logger.info(f"Current run date and time: {current_datetime}")
        config_path = parsed_json['DB_config_path']
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        akana_headers = get_auth_headers()
        search_url = parsed_json['Api_Url']
        logger.info(f"Required input details has been fetched from config file")
        starttime = time.time()
        reslt = []
        main_df = get_apps()
        DBNAME, USER, PASS, HOST, PORT = read_config_file(config_path, connection_profile)
        #conn = psycopg2.connect(dbname=DBNAME, user=USER, password=PASS, host=HOST, port=PORT)
        engine = sa.create_engine(f"postgresql://{USER}:{PASS}@{HOST}:{PORT}/{DBNAME}")
        conn = engine.connect()
        conn.autocommit = True
        logger.info(f'Database connections established sucessfully')
        main_df = main_df.where(pd.notnull(main_df), None)
        print(main_df.columns)
        Q1 = f"""truncate table {schemaname}.{tablename}"""
        engine.execute(Q1)
        logger.info(f'{schemaname}.{tablename} truncated sucessfully')
        main_df.to_sql(f"{tablename}",schema=schemaname,con=conn,if_exists='append',index=False,method='multi',chunksize=10000)
        logger.info(f'Successfully data loaded to RS')
        endtime = time.time()
        script_executed_time = endtime - starttime
        logger.info(f'Total time of script execution : {script_executed_time}')

    except Exception as e:
        print(e)
        os.system(f'echo "{Message_Ingestion}{e}{logfile}" | mailx -s "{subject}" {dl}')
#######################################
import requests,json,csv
import pandas as pd
import configparser
from datetime import date,datetime, timedelta
import sqlalchemy as sa
from sqlalchemy import types
import logging
import sys
from io import BytesIO
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

#######logfile #####
logfile = data['logfile']

############# Logging #########
with open(f'{logfile}', 'w', newline='') as csvfl:
    write_csv = csv.writer(csvfl, delimiter='|')
    write_csv.writerow(['\t TimeStamp \t', 'Severity', '\t Message \t'])

logging.basicConfig(filename=logfile,level=logging.INFO, format=('%(asctime)s | %(levelname)s | %(message)s'))
logger = logging.getLogger(logfile)

try :
    ##############data base connection method###############
    def read_config_file(filepath, connection):
        config = configparser.ConfigParser()
        config.read(filepath)
        db_name = config[connection]['dbname']
        user = config[connection]['user']
        password = config[connection]['password']
        host = config[connection]['host']
        port = int(config[connection]['port'])
        return db_name,user,password,host,port

    #### get smartsheet url and token ###
    def smartsheets_details(smartsheetpath):
        config = configparser.ConfigParser()
        config.read(smartsheetpath)
        token=config["smartsheets"]["Token"]
        url=config["smartsheets"]["url"]
        return token,url

    ######## calling Connections Method from dbconnections
    DBNAME, USER, PASS, HOST, PORT = read_config_file(config_path, connection_profile)
    engine = sa.create_engine(f"postgresql://{USER}:{PASS}@{HOST}:{PORT}/{DBNAME}")
    conn = engine.connect()
    conn.autocommit = True
    logging.info('Database connection successful')
    curt_date  = datetime.today()
    cur_date=datetime.now().strftime('%Y_%m_%d-%H_%M')

    ######reading token and url from root
    #Token,URL = smartsheets_details(smartsheet_path)
    #token=Token

    #### Getting data from Smartsheet url
    proxy = { "https": "https://http-proxy.health.ge.com:88"}
    hedrs = { "Authorization" : f"Bearer {token}"}
    payload = {}

    for id,tablename in zip(id_lst,Tablenames):
        try:
            url = f"{URL}{id}"
            response = requests.get(url, headers=hedrs,data=payload)
            if response.ok == True :
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
                            record.update({columns[cell['columnId']]:None})
                    records.append(record)

                df = pd.DataFrame(records)
                df['ingestion_timestamp'] = curt_date
                df.columns=pd.Series(df.columns).replace(' ','_',regex=True).str.lower()
                df = df.where(pd.notnull(df), None)
                logging.info(f' Total rows and columns in samrtsheet : {df.shape}')
                tablename=tablename.lower()
                exist_query=f"""SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = '{schema_name}' AND tablename = '{tablename}');"""
                STATUS = engine.execute(exist_query).fetchall()
                if(STATUS[0][0] == True or STATUS[0][0] == 't'):
                    truncate_query=f"""truncate table {schema_name}.{tablename}"""
                    engine.execute(truncate_query)
                    logging.info(f"the data in the {schema_name}.{tablename} is truncated ")
                df.to_sql(f"{tablename}",schema=schema_name,con=conn,if_exists='append',index=False,method='multi',chunksize=chunk)
                if Touchfile_required.lower() == 'yes':
                    os.system(f'touch {touchfile}redshift-{projectname}-{tablename}-{cur_date}.csv')
                    os.system(f'aws s3 cp {touchfile}redshift-{projectname}-{tablename}-{cur_date}.csv {s3path} --profile {s3_profile}')
                logging.info(f'data is loaded into redshift : {schema_name}.{tablename}')
            else:
                logging.info(f'Access to the Smartsheet Failed : {tablename} : {response}')
        except Exception as e :
            logging.error(f'Exception due to this error : {e}')
            os.system(f'echo "{Message_Ingestion}{logfile} . Below is the error for exception : {str(e)[:500]}" | mailx -s "{subject}"  {dl}')
except Exception as e :
    logging.error(f'Exception due to this error : {e}')
    os.system(f'echo "{Message_Ingestion}{logfile}. Below is the error for exception : {str(e)[:500]}" | mailx -s "{subject}"  {dl}')
######################
[hvr@ip-10-242-109-196 ~]$ cat /home/hvr/US174864/lgs_brokerspend_region_country_update.py
import os
import sys
import json
import requests
import glob
import time
import configparser
import psycopg2
import sqlalchemy as sa
from boxsdk import JWTAuth, Client
from datetime import datetime, timezone, timedelta
import pandas as pd
import psycopg2.extras as extras

boxfolderid = sys.argv[1]
targetpath = sys.argv[2]
configpath = sys.argv[3]
patteren = sys.argv[4]
filename = sys.argv[5]
schemaname = sys.argv[6]
tablename = sys.argv[7]
ingestiondl = sys.argv[8]
config_path = sys.argv[9]
connection_profile = sys.argv[10]
archifolderid = sys.argv[11]


def read_config_file(filepath, connection):
    config = configparser.ConfigParser()
    config.read(filepath)
    db_name = config[connection]['dbname']
    user = config[connection]['user']
    password = config[connection]['password']
    host = config[connection]['host']
    port = int(config[connection]['port'])
    return db_name, user, password, host, port


DBNAME, USER, PASS, HOST, PORT = read_config_file(config_path, connection_profile)


def getAccessToBox(JWT_file_path):
    auth = JWTAuth.from_settings_file(JWT_file_path)
    client = Client(auth)
    print("END at", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), ":", "getAccessToBox")
    return client


def getFileWithChangeName(client, file_id, dest_location, box_folder_id):
    file_name = client.file(file_id).get().name
    file_name1 = file_name.lower()
    x = file_name1.find("brokerspend_")
    if x != -1:
        print(file_name)
        open_file = open(dest_location + file_name, 'wb')
        client.file(file_id).download_to(open_file)
        open_file.close()
        time.sleep(60)
        with open(dest_location + file_name,encoding='utf-8',errors='ignore') as f:
            lines_after_17 = f.readlines()
            lines = lines_after_17
            print(file_name[:-3])
            with open(dest_location + file_name[:-3] + 'csv', 'w', encoding='utf-8') as f:
                f.writelines([str(line) for line in lines])
            df = pd.read_csv(dest_location + file_name[:-3] + 'csv',
                             delimiter="\t")
            df.to_csv(dest_location + file_name[:-3] + 'csv', encoding='utf-8', index=False)


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


def move_to_archive(archive_client, file_id, destination_folder_id, filename):
    file_id = file_id
    print("main file name :"+filename)
    destination_folder_id = destination_folder_id
    for item in archive_client.folder(destination_folder_id).get_items():
        if item.type == "file":
            id = item.id
            file_name = client.file(id).get().name
            #print("archive file name :" + file_name)
            if file_name == filename:
                folderid = item.id
                archive_client.file(file_id=folderid).delete()
                print("archive file name :" + file_name)
                print(folderid)
    file_to_move = archive_client.file(file_id)
    destination_folder = archive_client.folder(destination_folder_id)
    moved_file = file_to_move.move(parent_folder=destination_folder)
    print(f'File "{moved_file.name}" has been moved into folder "{moved_file.parent.name}"')

if __name__ == "__main__":
    try:
        path = targetpath
        showpadfiles = glob.glob(path + '*.*')
        for f in showpadfiles:
            os.remove(f)
        client = getAccessToBox(configpath)
        getFiles(client, boxfolderid, targetpath, f'*')
        os.chdir(targetpath)
        print(targetpath)
        for file in glob.glob("*.csv"):
            print(file)
            file1 = file[:-4]
            filename = file[:-3] + 'txt'
            print(filename)
            df = pd.read_csv(file)
            df.columns = df.columns.str.lower()
            df = df.replace(',', '', regex=True)
            df.columns = df.columns.str.lower()
            df.columns = df.columns.str.replace('$', '')
            df.columns = df.columns.str.replace('#', '')
            df.columns = df.columns.str.replace('(', '')
            df.columns = df.columns.str.replace(')', '')
            df.columns = df.columns.str.replace('  ', '_')
            df.columns = df.columns.str.replace(' ', '_')
            df.rename(columns={'ior/bn': 'iorbn'}, inplace=True)
            df.replace(r'\.0$', '', regex=True)
            df['data_origin'] = file[:-3] + 'txt'
            df['posting_agent'] = schemaname
            df['load_dtm'] = datetime.now()
            print(df.columns)
            df = df.where(pd.notnull(df), None)
            conn = psycopg2.connect(dbname=DBNAME, user=USER, password=PASS, host=HOST, port=PORT)
            Q1 = f"""delete from {schemaname}.{tablename} where data_origin='{filename}'"""
            print(Q1)
            cursor = conn.cursor()
            cursor.execute(Q1)
            conn.commit()
            execute_values(conn, df, schemaname + '.' + tablename)
            print("Successfully data loaded to RS")
            folder_id = boxfolderid
            client = getAccessToBox(configpath)
            for item in client.folder(folder_id).get_items():
                if item.type == "file":
                    file2 = item.name[:-4]
                    if file2 == file1:
                        destination_folder_id = archifolderid
                        move_to_archive(client, item.id, destination_folder_id, item.name)
    except Exception as e:
        print("some error occurred while executing")
        print(e)
        os.system(
            f'echo "lgs_brokerspend_region_country" | mailx -s "lgs_brokerspend_region_country" {ingestiondl} ')
######################################

import json
import requests
import argparse
import boto3
import pandas as pd
from io import BytesIO
from datetime import date, datetime, timedelta
import os

parser = argparse.ArgumentParser()
parser.add_argument('--infile', nargs=1, help="JSON file to be processed", type=argparse.FileType('r'))
args = parser.parse_args()
contents = json.load(args.infile[0])
try:
    token = contents["token"]
    url_link = contents["url"]
    id = contents["id"]
    s3_bucket_name = contents["s3_bucket"]
    s3_folder = contents["s3_folder"]
    profile=contents["profile"]
    Message_Ingestion = contents["Email_info"]["Message_Ingestion"]
    dl = contents["Email_info"]["dl"]
    subject = contents["Email_info"]["subject"]
    logfile = contents["logfilepath"]
    dt=datetime.now()
    year, month, day , hour= dt.strftime('%Y'), dt.strftime('%m'), dt.strftime('%d') ,dt.strftime('%H')
    headers = {"Authorization": f"Bearer {token}", "Accept": "text/csv"}
    session = boto3.Session(profile_name=profile)
    s3_client = session.client('s3')
    url = f"{url_link}{id}"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    file_content = response.content
    df = pd.read_csv(BytesIO(file_content))
    row_count = len(df)
    print(row_count)
    filename = f"{id}.csv"
    file_name = BytesIO(file_content)
    s3_key = f"{s3_folder}{id}/year={year}/month={month}/day={day}/hour={hour} /{filename}"
    s3_client.upload_fileobj(file_name, s3_bucket_name, s3_key)
    print(f"File {filename} uploaded to S3 bucket {s3_bucket_name}/{s3_key} successfully.")
except Exception as e:
    print(f"Error processing file {filename} for ID {id}: {e}")
    os.system(f'echo "{Message_Ingestion}{e}  logfile path : {logfile}" | mailx -s "{subject}" {dl}')

################################

import pandas as pd
from sqlalchemy import types
import sys
import json
import os
from datetime import datetime

def sqlcol(dfparam):
    dtypedict = {}
    for i, j in zip(dfparam.columns, dfparam.dtypes):
        if "object"in str(j):
            if dfparam[i].str.len().max() >= 256:
                dtypedict.update({i: types.VARCHAR(65000, collation='case_insensitive')})
            else:
                dtypedict.update({i: types.VARCHAR(256, collation='case_insensitive')})
    return dtypedict

if __name__=="__main__":
    arg1=sys.argv[1]
    config_data_path=arg1
    with open (config_data_path ,'r') as json_file:
        parsed_data=json.load(json_file)
        config_path=parsed_data['config_path']
        connection_profile=parsed_data['connection_profile']
        schemaname=parsed_data['schemaname']
        tablename=parsed_data['tablename']
        dl=parsed_data['dl']
        filepath=parsed_data['filepath']
        utils_path=parsed_data['utils_path']
        dl=parsed_data['dl']
    sys.path.append(utils_path)
    parent_path = os.path.dirname(os.path.abspath(__file__))
    from utils import setup_logger, send_email_notification,get_connection
    log_filename = str(os.path.abspath(__file__)).split('/')[-1].replace('py', 'log')
    log_path=os.path.join(parent_path,log_filename)
    logger = setup_logger(log_path)
    try:
        logger.info("Ingestion Started")
        engine = get_connection(config_path,connection_profile)
        truncate_query=f"""truncate table {schemaname}.{tablename}"""
        engine.execute(truncate_query)
        logger.info(f"{schemaname}.{tablename} has been truncated")
        df= pd.read_csv(filepath,delimiter='|',engine='python')
        df['hvr_last_upd_tms'] = datetime.now()
        df.columns=pd.Series(df.columns).replace(' ','_',regex=True).str.lower()
        df.columns=pd.Series(df.columns).replace('#','',regex=True).str.lower()
        df.to_sql(name=tablename,schema=schemaname,con=engine,dtype=sqlcol(df),if_exists='append',index=False,method='multi',chunksize=20000)
        send_email_notification(message=f"{filepath} has been processed successfully on {datetime.today().strftime('%Y-%m-%d')} at {datetime.today().strftime('%H:%M:%S')}",subject=f"HSA Sftp Ingestion successful for - {schemaname}.{tablename}",email_stake_holders=dl)
        logger.info("Data ingestion has been completed")
    except Exception as e:
        logger.error(f"Exception occured-> {e}")
        send_email_notification(message=f"Exception -> {e} occured at {os.path.abspath(__file__)}", subject=f"HSA Sftp Prod Ingestion Failure",log_path=log_path)
#########################################

{"config_path":"/home/hvr/.aws/redshift_connection.ini",
"connection_profile":"Connections_PROD",
"schemaname":"hdl_hsa_flat_file",
"filepath":"/hvr/sftp/SrcFiles/ofs/HR_HSAT_Training_URA.txt",
"tablename":"hr_hsat_training_ura",
"dl":"healthbiHsaOdpdevteam@ge.com,IT_CDO_Regions_USCAN_Notification@gehealthcare.com",
"utils_path":"/hvr/sftp/SrcFiles/ofs/hsa_hr_ura/"}
[hvr@ip-10-242-109-196 ~]$ cat /hvr/sftp/SrcFiles/ofs/hsa_hr_ura/hsa_ura.py
import pandas as pd
from sqlalchemy import types
import sys
import json
import os
from datetime import datetime

def sqlcol(dfparam):
    dtypedict = {}
    for i, j in zip(dfparam.columns, dfparam.dtypes):
        if "object"in str(j):
            if dfparam[i].str.len().max() >= 256:
                dtypedict.update({i: types.VARCHAR(65000, collation='case_insensitive')})
            else:
                dtypedict.update({i: types.VARCHAR(256, collation='case_insensitive')})
    return dtypedict

if __name__=="__main__":
    arg1=sys.argv[1]
    config_data_path=arg1
    with open (config_data_path ,'r') as json_file:
        parsed_data=json.load(json_file)
        config_path=parsed_data['config_path']
        connection_profile=parsed_data['connection_profile']
        schemaname=parsed_data['schemaname']
        tablename=parsed_data['tablename']
        dl=parsed_data['dl']
        filepath=parsed_data['filepath']
        utils_path=parsed_data['utils_path']
        dl=parsed_data['dl']
    sys.path.append(utils_path)
    parent_path = os.path.dirname(os.path.abspath(__file__))
    from utils import setup_logger, send_email_notification,get_connection
    log_filename = str(os.path.abspath(__file__)).split('/')[-1].replace('py', 'log')
    log_path=os.path.join(parent_path,log_filename)
    logger = setup_logger(log_path)
    try:
        logger.info("Ingestion Started")
        engine = get_connection(config_path,connection_profile)
        truncate_query=f"""truncate table {schemaname}.{tablename}"""
        engine.execute(truncate_query)
        logger.info(f"{schemaname}.{tablename} has been truncated")
        df= pd.read_csv(filepath,delimiter='|',low_memory=False)
        df['hvr_last_upd_tms'] = datetime.now()
        df.to_sql(name=tablename,schema=schemaname,con=engine,dtype=sqlcol(df),if_exists='append',index=False,method='multi',chunksize=20000)
        send_email_notification(message=f"{filepath} has been processed successfully on {datetime.today().strftime('%Y-%m-%d')} at {datetime.today().strftime('%H:%M:%S')}",subject=f"HSA Sftp Ingestion successful for - {schemaname}.{tablename}",email_stake_holders=dl)
        logger.info("Data ingestion has been completed")
    except Exception as e:
        logger.error(f"Exception occured-> {e}")
        send_email_notification(message=f"Exception -> {e} occured at {os.path.abspath(__file__)}", subject=f"HSA Sftp Prod Ingestion Failure",log_path=log_path)
##############################################

[hvr@ip-10-242-109-196 ~]$ cat /data/custom_scripts/hybrismaster_prod/fullload.sh
#!/bin/bash
    python3 /data/custom_scripts/hybrismaster_prod/product.py
    python3 /data/custom_scripts/hybrismaster_prod/user.py
[hvr@ip-10-242-109-196 ~]$ cat /data/custom_scripts/hybrismaster_prod/product.py
import boto3
import io
from datetime import datetime
import time
from azure.storage.blob import BlobServiceClient

profile = 'prod'
s3_bucket = 'odp-us-prod-ent-raw'
s3_prefix = 'hybris'

azure_storage_account_name = 'mr0wdkdqnx42qrsxg19gjzl'
azure_storage_account_key = 'xJrJ6ca9E19S4asCmTRUO204fDgTNJfDM15FuwfRExmHAhdDbwLXesQsBN5oHEY6Jol5aVe8s96YgqkBDVKbIQ=='
container_name = 'hybris'
odp_folder = 'NFS_DATA/hybris-media/report/odp/productDetailsReport/'

session = boto3.Session(profile_name=profile)
s3 = session.client('s3')

blob_service_client = BlobServiceClient(account_url=f"https://{azure_storage_account_name}.blob.core.windows.net/", credential=azure_storage_account_key)
clientContainer = blob_service_client.get_container_client(container_name)
list_response = clientContainer.list_blobs(name_starts_with=odp_folder)

start_time = time.time()
for r in list_response:
    try:
        timestamp = datetime.now()
        day = timestamp.day
        month = timestamp.month
        year = timestamp.year
        target_file_name = f"Product-Details-Report_{timestamp.strftime('%Y%m%d%H%M%S')}.json"
        s3_path = f"{s3_prefix}/year={year}/month={month:02}/day={day:02}/{target_file_name}"
        blob_client = clientContainer.get_blob_client(r.name)
        download_stream = blob_client.download_blob()
        data = download_stream.readall()
        # Upload data to S3
        s3.put_object(Bucket=s3_bucket, Key=s3_path, Body=data)
        print(s3_path)
        time.sleep(5)
    except ValueError as v_err:
        continue
    except Exception as err:
        continue
    finally:
        data=''


end_time = time.time()
time_taken = end_time - start_time

print(f"{time_taken} time taken to complete ingestion to s3")
#############################
import os, sys
from datetime import datetime
import argparse
import json
import requests
import io
import pandas as pd
import argparse, json, traceback
import argparse


def auth(url, headers):
    try:
        logger.info("Executing auth function")
        response = requests.get(url, headers)
        logger.info("Authentication Successful")
        return response
    except Exception as e:
        logger.error(f"Failed to execute auth function with error --> {e}")
        raise


def main(config):
    try:
        logger.info("Executing main function")
        response = auth(config["url"], headers=config["headers"])
        buffer_data = io.BytesIO(response.content)
        df = pd.read_excel(buffer_data, engine='openpyxl')
        if "s3_bucket_name" and "s3_prefix_name" in config:
            from s3_operations import S3Operations
            s3 = S3Operations(logger=logger, profile_name=config["s3_profile"], partition=config["s3_partition"])
            s3.upload_file(
                file_name=config["file_name"],
                file_type=config["file_type"],
                data=df,
                bucket=config["s3_bucket_name"],
                prefix=config["s3_prefix_name"])
            logger.info(f"Ingestion to S3 completed")
        if "main_table_name" and "schema_name" in config:
            df.columns = pd.Series(df.columns).replace(" ", "_", regex=True).str.lower()
            df["ingestion_timestamp"] = datetime.today()
            from redshift_loader import Database
            Database(load_type=config.get("load_type", "truncate_and_load"), logger=logger,
                     config=config["redshift_config"], profile=config["redshift_profile"], data=df,
                     schema=config["schema_name"], main_table_name=config["main_table_name"])
            logger.info(f"Ingestion to Redshift Completed")
    except Exception as e:
        logger.error(f"Failed to execute main function with --> {e}")
        raise


if __name__ == "__main__":
    start_time = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    parser = argparse.ArgumentParser(description="Download data and save to a file.")
    parser.add_argument('--infile', required=True, help="JSON file with configuration", type=argparse.FileType('r'))
    args = parser.parse_args()
    config_file = args.infile
    config = json.load(config_file)
    sys.path.insert(0, config['utils_path'])
    from utils import setup_logger, send_email_notification

    log_filename = str(args.infile.name).split('/')[-1].replace('.json', '.log')
    log_filename = str(log_filename.replace(".log", f"_{datetime.today().strftime('%Y_%m_%d_%H_%H_%S')}.log"))
    log_path = os.path.join(config["log_file_path"], log_filename)
    logger = setup_logger(log_path)
    logger.info("Ingestion Started")
    try:
        main(config)
        send_email_notification(
            message=f"Ingestion Completed \n Script Path-> {os.path.abspath(__file__)} \n Config Path-> {args.infile.name} \n Log Path-> {log_filename} ",
            subject=f"SUCCESS | {config['environment']} | {config.get('source')} Ingestion | {config['main_table_name']} | {config['redshift_profile']}",
            log_path=log_path, logger=logger)
        logger.info("Ingestion Completed")
    except Exception as e:
        logger.error(f"Exception occured-> {e} ")
        log_file_size = os.path.getsize(log_path)
        file_size = log_file_size / (1024 * 1024)
        if file_size < 1:
            logger.error(f"Exception occurred -> {e}")
            send_email_notification(
                message=f"Exception -> {e} \n Script Path-> {os.path.abspath(__file__)} \n Config Path-> {args.infile.name} \n Log Path-> {log_filename}  {traceback.format_exc()}",
                subject=f"FATAL | {config['environment']} | {config.get('source')} Ingestion | {config['main_table_name']} | {config['redshift_profile']}",
                log_path=log_path, logger=logger)
        else:
            send_email_notification(
                message=f"Exception -> {e} \n Script Path-> {os.path.abspath(__file__)} \n Config Path-> {args.infile.name} \n Log Path-> {log_filename} \n Couldn't attach log file due to file size limitation refer to log path mentioned  {traceback.format_exc()}",
                subject=f"FATAL | {config['environment']} | {config.get('source')} Ingestion | {config['main_table_name']} | {config['redshift_profile']}",
                logger=logger)
################################################



#### Importing Necessary Packages ####
import sys,os
from datetime import datetime
from io import BytesIO
from boxsdk import JWTAuth, Client
import pandas as pd
import argparse
import json
import subprocess
import boto3
import traceback
from time import sleep



### Sample Config File ###
{
    "sftp_path":"Source sftp path location",
    "source":"source name",
    "environment":"dev / Prod",
    "file_pattern":"file capture pattern. file name should contain only one '*' special char to match pattern",
    "ingestion_audit_field":"Name of the field that will be added in table as ingestion audit",
    "log_file_path":"log path where log file has to be created",
    "schema_name":"Target Schema name",
    "main_table":"Target Table name",
    "file_type":"csv / excel / text has to be mentioned",
    "stage_table":"Target Stage table name keep it empty if not required but it has be present in config",
    "data_origin":"Name of the column which will store source box file id as value in Table",
    "posting_agent":"Name of the column which will store source box file name as value in Table",
    "required_excel_features":"any extra attributes need to read_excel can be added here as a dict eg - {'method':'value'}",
    "required_csv_features":"any extra attributes need to read_csv can be added here as a dict eg - {'method':'value'}",
    "load_type":"incremental/truncate_and_load/fullload",
    "redshift_config":"Path to redshift configuration",
    "add_on_email_stake_holders":"Only to be added when extra email stake holders had to be included or else neglect this key value",
    "utils_path":"Path to the utility py file which has logging and alert feature on failure compulsory input in config",
    "column_names": "Target table column names (optional - if it is a requirement to add columns then add this field otherwise please remove the filed from json file)",
    "primary_key":"Only need for incremental should be kept as empty if not required config expects this parameter",
    "manifest_archival_s3_bucket" : "s3 bucket name to store archive manifest files",
    "manifest_archival_s3_prefix_file_path" : "S3 path to store archive manifest files under the s3 path",
    "manifest_archival_s3_profile" : "s3 connection profile",
    "file_archival_s3_bucket" : "archival files s3 bucket name (optional - we need to add this value if only files have to copy the archive s3 location from sftp)" ,
    "file_archival_s3_prefix_path" : "archival s3 bucket prefix",
    "file_archival_s3_profile" : "archival s3 bucket prefix",
    "file_archival_path_partiation" : "archived files path prefix yes or no, If value is 'y' files will place under date partiation path or if value is 'n' then files will place under provided prefix path",
    "touchfile_required": "y or n need to mentioned, if it is 'y' then need to provide all below metioned key and values or if it is 'n' then below (touchfile_s3_profile/touchfile_bucket/touchfile_bucket_prefix/touchfile_s3_path_partiation) keys and value is not required",
    "touchfile_s3_profile":"s3 profile for touch file store s3 bucket",
    "touchfile_bucket":"bucket name for touch file store",
    "touchfile_bucket_prefix":"bucket prefix name path",
    "touchfile_s3_path_partiation":"it sould have value (y/n), to manage s3 path partiation"

}


class DataFetcher:
    # A class to fetch files from SFTP and ingest into Redshift depending on the inputs provided
    def __init__(self, config, logger) -> None:
        """
        The Constructor for DataFetcher class.

        Parameters:
        config (dict): Configuration dictionary
        logger (Logger): Logger object
        engine (object): Redshift connection engine
        """
        self.config = config
        self.logger = logger


    def get_excel_sftp(self, sftp_path, df_list_name):
        """
        A method to read excel file from sftp location and return the data frame

        Parameters:
        sftp_path (str) : Source sftp path location

        Returns:
        df (DataFrame) : DataFrame which holds data fetched from sftp location
        sftp (str)   : Source sftp path
        item(str)    : Source file name that has been processing

        """

        df = pd.DataFrame()
        main_df = pd.DataFrame()
        for item in os.listdir(sftp_path):
            if os.path.isfile(f"{sftp_path}/{item}") and item == df_list_name[0]:
                if "required_excel_features" in self.config:
                    if "sheet_names" in self.config:
                        for sheet in self.config["sheet_names"].split(','):
                            main_df = pd.read_excel(f"{sftp_path}/{item}",sheet_name=sheet,**self.config["required_excel_features"])
                            df = pd.concat([df, main_df])
                        main_df = df
                    else:main_df=pd.read_excel({sftp_path}/{item},**self.config["required_excel_features"])
                else:main_df=pd.read_excel({sftp_path}/{item})
                raw_df = main_df.copy()
                if "column_names" in self.config:main_df.columns=self.config["column_names"]
                main_df["data_origin"]=sftp_path
                main_df["posting_agent"]=item
        return main_df,raw_df

    def get_csv_sftp(self, sftp_path, df_list_name):
        """
        A method to read csv file from sftp location and return the data frame

        Parameters:
        client (object) : Client object initiated for box authentication
        folder_id (str) : Box folder id

        Returns:
        df (DataFrame) : DataFrame which holds data fetched from box location
        items (list)   : Box file id or ids if multiple files are provided
        """
        main_df = pd.DataFrame()
        raw_df = pd.DataFrame()
        for item in os.listdir(sftp_path):
            if os.path.isfile(f"{sftp_path}/{item}") and item == df_list_name[0]:
                if "required_csv_features" in self.config:main_df = pd.read_csv(f"{sftp_path}/{item}",**self.config["required_csv_features"])
                else:main_df = pd.read_csv(f"{sftp_path}/{item}")
                raw_df = main_df.copy()
                if "column_names" in self.config:main_df.columns=self.config["column_names"]
                main_df[self.config["data_origin"]]=sftp_path
                main_df[self.config["posting_agent"]]=item
        return main_df,raw_df

    def copy_files_s3_archive(self,filename,s3_ops,data):
        """
        A method is used to copy the file from sftp to s3 archival location
        Parameters :
            filename (str) : name of the filename
            s3_ops (object) : s3 operations module object
            data(DataFrame) : Dataframe of file data to be upload to s3

        Return : None

        """
        try:
            file_names = filename.split('.')
            renamed_filename = f"{file_names[0]}-{year}_{month}_{day}.{file_names[1]}"
            if self.config["file_type"].lower() == 'text' or self.config["file_type"].lower() == 'csv':
                file_type = 'csv'
            else:
                file_type = self.config["file_type"]
            if "delimiter" in self.config["required_csv_features"]:
                s3_ops.upload_file(renamed_filename,file_type,data,self.config["file_archival_s3_bucket"],self.config["file_archival_s3_prefix_path"],output_delimiter=self.config["required_csv_features"]["delimiter"])
            else:
                s3_ops.upload_file(renamed_filename,file_type,data,self.config["file_archival_s3_bucket"],self.config["file_archival_s3_prefix_path"])
        except Exception as e:
            self.logger.info(f"Exception occured while copying files to archive s3 location error : {e}")
            raise

    def touch_file(self):
         #initialization touchfile module
        try:
            touch = touch_file.touchfile(logger=self.logger,tablename=self.config["main_table"],profile_name=self.config["touchfile_s3_profile"],partition=self.config["touchfile_s3_path_partiation"])
            touch.touchfile_to_s3(self.config["touchfile_bucket"],self.config["touchfile_bucket_prefix"])
        except Exception as e:
            self.logger.info(f"Error occured while creating touch file error : {e}")
            raise


    def move_manifestfile_to_s3(self):
        """
        A method is used to move manifest file to s3 location
        Parameters : None
        Retunr : None

        """
        try:
            local_sftp_path = f"{self.config['manifestfile_path']}{self.config['main_table']}{year}{month}{day}.txt"
            data = pd.read_csv(local_sftp_path)
            s3 = s3_operations.S3Operations(logger=self.logger,profile_name=self.config["manifest_archival_s3_profile"],partition='y')
            file_type = 'csv'
            s3.upload_file(f"{self.config['main_table']}{year}{month}{day}.txt",file_type,data,self.config["manifest_archival_s3_bucket"],self.config["manifest_archival_s3_prefix_file_path"])
            for item in os.listdir(self.config["manifestfile_path"]):
                if item.startswith(self.config['main_table']):
                   os.remove(f"{self.config['manifestfile_path']}{item}")
            self.logger.info("manifest txt file has been moved to s3 bucket")
        except Exception as e:
            self.logger.info("Exception occured while moving manifestfile from local to s3 bucjet error : {e}")
            raise

    def generate_manifest_file(self,df):
        """
        A method is used to create manifest text file in local
        Params:
           df Dataframe : Dataframe
           Return : None
        """
        try:
            df.to_csv(f"{self.config['manifestfile_path']}{self.config['main_table']}{year}{month}{day}.txt", index=None,mode='w')
            self.logger.info("manifest text file has been created")
        except Exception as e:
            self.logger.info(f"Exception occured while generating manifest file error : {e}")


    def pattern_csv_or_excel(self,item,s3_ops):
        """
        A method is used to call file_type(i.e csv,text and excel) functions to achive pattern type files loading

        Parameter:
            df_list1 (list) : list of dataframe column values
        Return :
            None

        """
        try:
            if self.config["file_type"].lower()=='csv' or self.config["file_type"].lower()=='text':
                df,raw_df = self.get_csv_sftp(self.config["sftp_path"],item)
            elif self.config["file_type"].lower()=='excel':
                df,raw_df = self.get_excel_sftp(self.config["sftp_path"],item)
            if not df.empty:
                if self.config["replace_space_in_column_name"].lower()=='y':
                    df.columns = pd.Series(df.columns).replace(' ', '_', regex=True).str.lower()
                else:
                    df.columns = pd.Series(df.columns).str.lower()
                df[self.config["ingestion_audit_field"]] = datetime.today()
                if "schema_name" and "main_table" in self.config:
                    Database(load_type=item[1],logger=logger,config=self.config["redshift_config"],profile=self.config["redshift_profile"],data=df,schema=self.config["schema_name"],main_table_name=self.config["main_table"],stage_table_name=self.config["stage_table"],primary_key=self.config["primary_key"])
                self.logger.info(f"{item[0]} Ingestion Completed")
                if "file_archival_s3_bucket" in self.config and "file_archival_s3_prefix_path" in self.config and "file_archival_s3_profile" in self.config and "file_archival_path_partiation" in self.config:
                    #print(raw_df)
                    self.copy_files_s3_archive(item[0],s3_ops,raw_df)

            else:self.logger.info("No data to ingest")
        except Exception as e:
            raise ValueError(f"Exception occured while loading filename : {item[0]} error : {e} ")

    def main(self):
        """
        A method to initiate Box ingestion based on the inputs provided in config

        Parameters: None

        Returns: None
        """
        try:
            #initialization indirect_file_names_db_operations
            data_operations = db_ops.db_operations(logger=self.logger,file_pattern = self.config["file_pattern"],source_system_type = self.config["source_system_type"],source_system = self.config["source_name"],source_system_path=self.config["sftp_path"],target=f"{self.config['schema_name']}.{self.config['main_table']}",pattern_load_type=self.config["load_type"])

            #initialization indirect_file_names_fetcher module
            file_operations = file_ops.pattern_files_loader(logger=self.logger,file_pattern=self.config["file_pattern"],sftp_path=self.config["sftp_path"])

            #initialization s3_operations module
            if "file_archival_s3_bucket" in self.config and "file_archival_s3_prefix_path" in self.config and "file_archival_s3_profile" in self.config and "file_archival_path_partiation" in self.config:
                s3_ops = s3_operations.S3Operations(logger=self.logger,profile_name=self.config["file_archival_s3_profile"],partition=self.config["file_archival_path_partiation"])
            else:
                s3_ops = None

            self.logger.info(f"Searching for any last run pending files to be processed")
            pending_files = data_operations.fetch_pattern_files()
            if len(pending_files)==0:
                self.logger.info(f"No pending filenames are avaliable in audit runtime table")
                list_files=file_operations.get_sftp_patternfiles()
                removed_duplicated_files=data_operations.remove_already_processed_files(list_files)
                df1 = data_operations.generate_insert_table_dataframe(removed_duplicated_files)
                data_operations.insert_records(df1)
                new_files = data_operations.fetch_pattern_files()
                if len(new_files) == 0:
                    self.logger.info(f"No new files avaliable in source loaction")
                    sys.exit()
                self.generate_manifest_file(df1)
                for item in new_files:
                    self.pattern_csv_or_excel(item,s3_ops)
                    data_operations.move_data(item[0])
                self.move_manifestfile_to_s3()
                if self.config["touchfile_required"].lower() == 'y':
                    self.touch_file()
                elif self.config["touchfile_required"].lower() == 'n':
                   self.logger.info(f"Provided touchfile_required as 'n', So touch file creation process skipped")
                self.logger.info(" Data ingestion process has been completed - SUCCESS")
            elif len(pending_files) > 0:
                self.logger.info(f"Pending files has been indentified -- Process started")
                for item in pending_files:
                    self.pattern_csv_or_excel(item,s3_ops)
                    data_operations.move_data(item[0])
                self.move_manifestfile_to_s3()
                if self.config["touchfile_required"].lower() == 'y':
                    self.touch_file()
                elif self.config["touchfile_required"].lower() == 'n':
                   self.logger.info(f"Provided touchfile_required as 'n', So touch file creation process skipped")
                self.logger.info(" Data ingestion process has been completed - SUCCESS")
            # Sucess Email notification
            send_email_notification(message=f"Ingestion Sucessfull \n Script Path-> {os.path.abspath(__file__)} \n Config Path-> {arguments.infile[0].name}", subject=f"INFO - SUCCESS | {config['environment']} | {self.config['source_name']} Ingestion | SFTP - {config['sftp_path']} | {config['schema_name']}.{table_name} {config['redshift_profile']}",log_path=log_path,logger=logger,add_on_email_stake_holders=config.get("add_on_email_stake_holders", None))
            logger.info("Ingestion Completed")
        except Exception as e:
            self.logger.error(f"Failed to execute main method in DataFetcher class , error --> {e}")
            raise

if __name__ == "__main__":
    start = datetime.today().strftime("%Y-%m-%d_%H:%M:%S")
    year, month, day, time = datetime.today().strftime("%Y"), datetime.today().strftime("%m"), datetime.today().strftime("%d"), f"{datetime.today().strftime('%H')}-{datetime.today().strftime('%M')}-{datetime.today().strftime('%S')}"
    parser = argparse.ArgumentParser()
    parser.add_argument('--infile', nargs=1, help="JSON file to be processed", type=argparse.FileType('r'))
    arguments = parser.parse_args()
    config = json.load(arguments.infile[0])
    parent_path = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0,config["utils_path"])
    from utils import setup_logger, send_email_notification
    from redshift_connector import get_connection
    from redshift_loader import Database
    import indirect_file_names_fetcher as file_ops
    import indirect_file_names_db_operations_dev as db_ops
    import s3_operations as s3_operations
    import touchfile_creator as touch_file
    log_filename = str(arguments.infile[0].name).split('/')[-1].replace('json', 'log')
    log_filename=str(log_filename.replace(".log",f"_{datetime.today().strftime('%Y_%m_%d_%H_%H_%S')}.log"))
    log_path=os.path.join(config["log_file_path"], log_filename)
    logger = setup_logger(log_path)
    table_name=config.get("main_table")
    logger.info("Ingestion Started")
    try:
        data_fetcher = DataFetcher(config, logger)
        sys.exit(data_fetcher.main())
    except Exception as e:
        logger.error(f"Exception occurred: {e}")
        log_file_size= os.path.getsize(log_path)
        file_size=log_file_size / (1024 * 1024)
        if file_size < 1:
            send_email_notification(message=f"Script Path-> {os.path.abspath(__file__)} \n Config Path-> {arguments.infile[0].name} \n Exception -> {e} occured \n {traceback.format_exc()}", subject=f"FATAL | {config['environment']} | {config['source_name']} Ingestion | SFTP - {config['sftp_path']} | {config['schema_name']}.{table_name} {config['redshift_profile']}",log_path=log_path,logger=logger,add_on_email_stake_holders=config.get("add_on_email_stake_holders", None))
            sys.exit()
        else:
            send_email_notification(message=f"Script Path-> {os.path.abspath(__file__)} \n Config Path-> {arguments.infile[0].name} \n Exception -> {e} occured \n Log File couldn't be attached with this mail due to file size limit being exceeded, Log Path-> {log_path} \n {traceback.format_exc()}", subject=f"FATAL | {config['environment']} | {config['source_name']} Ingestion | SFTP - {config['sftp_path']} | {config['schema_name']}.{table_name} {config['redshift_profile']}",logger=logger,add_on_email_stake_holders=config.get("add_on_email_stake_holders", None))
            sys.exit()
######################

import pandas as pd
import json
import requests
from datetime import date, datetime, timedelta
import argparse

curt_date = date.today()
pre_date = date.today() - timedelta(days=1)


def access_tok(config):
    token_response = requests.get(config["url_cred"], proxies=config.get("proxyDict", {}))
    logger.info(f"response status code: {token_response.status_code}")
    if token_response.status_code != 200:
        logger.error(f"Failed to response. Status code: {token_response.status_code}")
    access_token = token_response.json().get("access_token")
    return access_token

def get_program_data(config, access_token):
    offset = config["offset"]
    uqProgramIdList = set()
    json_res = []
    while offset >= 0:
        stroff = str(offset)
        url = config["url"].format(
            access_token=access_token,
            maxReturn=config["maxReturn"],
            offset=stroff,
            pre_date=pre_date.strftime("%Y-%m-%d"),
            curt_date=curt_date.strftime("%Y-%m-%d"))

        logger.info(f"printing URL link: {url}")

        response = requests.get(url, proxies=config.get("proxyDict", {}))

        # stop if out of scope
        if "No assets found for the given search criteria." in response.text:
            print(f"No more data. End offset (out of limit): {stroff}")
            break

        json_res.extend(response.json().get("result"))

        # get all programIDs
        for obj in response.json().get("result"):
            uqProgramIdList.add(obj.get("id"))
        offset += config["maxReturn"]

        # just to track progress
        if offset % 1000 == 0:
            print(f"Current offset: {stroff}")

    df = pd.json_normalize(json_res)
    logger.info(f"Dataframe loaded with {len(df)} rows and {len(df.columns)} columns")

    return df, uqProgramIdList


def upload_to_s3(s3, config, df):
    try:
        s3.upload_file(
            file_name=config["file_name"],
            file_type=config["file_type"],
            data=df,
            bucket=config["s3_bucket_name"],
            prefix=config["s3_prefix_name"])

        logger.info(f"File {config['file_name']} uploaded successfully to {config['s3_bucket_name']}/{config['s3_prefix_name']}")
    except Exception as e:
        logger.error(f"Error uploading file: {str(e)[:500]}")
        raise

def load_df_to_db(df, config):
    logger.info("Starting to load data into the database.")
    db_loader = Database(
        load_type=config.get("load_type", "incremental"),
        logger=logger,
        config=config["redshift_config"],
        profile=config["redshift_profile"],
        data=df,
        schema=config["schema_name"],
        main_table_name=config["main_table_name"])
    logger.info(f"Data loaded successfully into table: {config['main_table_name']}")


def main(config):
    logger.info("Main execution started")
    access_token = access_tok(config)
    get_program_data(config, access_token)
    df = get_program_data(config, access_token)
    s3 = S3Operations(logger=logger, profile_name=config["s3_profile"], partition=config["s3_partition"])
    logger.info("S3 Operations initialized")
    upload_to_s3(s3, config, df)
    load_df_to_db(df, config)
    logger.info("Main execution finished")

if __name__ == "__main__":
    start_time = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    parser = argparse.ArgumentParser(description="Download data and save to a file.")
    parser.add_argument('--infile', required=True, help="JSON file with configuration", type=argparse.FileType('r'))
    args = parser.parse_args()
    config = json.load(args.infile)
    sys.path.insert(0, config['utils_path'])
    from utils import setup_logger, send_email_notification
    from s3_operations import S3Operations
    from redshift_loader import Database
    log_filename = str(args.infile.name).split('/')[-1].replace('.json', '.log')
    log_path = os.path.join(config["log_file"], log_filename)
    logger = setup_logger(log_path)
    try:
        main(config)
        logger.info("END")
    except Exception as e:
        logger.error(f"Exception occurred -> {e}")
        send_email_notification()
##################################################


import pandas as pd
import json
import os
import requests
from datetime import date, datetime, timedelta
import argparse
import traceback
import sys


def access_tok(url):
    try:
        logger.info("Executing access_tok")
        token_response = requests.get(url=url)
        logger.info(f"response status code: {token_response.status_code}")
        if token_response.status_code == 200:
            access_token = token_response.json().get("access_token")
            logger.error("Authentication Successful")
            logger.info(token_response.json())
            return access_token
        else:
            logger.error(f"Failed to authenticate {token_response.status_code} {token_response['message']}")
            raise Exception
    except Exception as e:
        logger.error(f"Failed to execute access_tok function with error --> {e} {traceback.format_exc()}")
        raise


def get_program_data(api_url, token):
    try:
        logger.info("Executing get_program_data function")
        uqProgramIdList = set()
        json_res = []
        offset = config["offset"]
        max_return = config["maxReturn"]
        while offset >= 0:
            logger.info("offset")
            stroff = str(offset)
            logger.info(offset)
            url = f"{api_url}?access_token={token}&maxReturn={max_return}&offset={str(offset)}&earliestUpdatedAt={pre_date.strftime('%Y-%m-%d')}T00:00:00-05:00&latestUpdatedAt={curt_date.strftime('%Y-%m-%d')}T00:00:00-05:00"
            logger.info(f"printing URL link: {url}")
            response = requests.get(url)
            if response.status_code == 200 and response.json()["success"] == True:
                if "result" in response.json():
                    logger.info(json_res)
                    json_res.extend(response.json().get("result"))
                    for obj in response.json().get("result"):
                        uqProgramIdList.add(obj.get("id"))
                    offset += max_return  # changes the offset value to paginate
                    logger.info(offset)
                    if offset % 1000 == 0:
                        logger.info(f"Current offset: {stroff}")  # progress tracker
                else:
                    warning = response.json().get("warnings", [])  # Captures if any warning raised in api response
                    if len(warning) > 0:
                        logger.warning(f"{warning}")
                        logger.info("No Data to Fetch")
                        break
            else:
                logger.error(f"Failed to authenticate with status code - {response.status_code}")
                errors = response.json().get("errors", [])  # Captures if any errors raised in api response
                if len(errors) > 0:
                    for res in errors:
                        logger.error(
                            f"Failed to Authenticate with error code - {res.get('code', 'could not capture error code')} \n message - {res.get('message', 'could not capture error message')}")
                else:
                    logger.error(f"Error Occured ")
                raise
        df = pd.DataFrame(json_res)  # constructing a dataframe from the response fetched
        logger.info(f"Dataframe loaded with {len(df)} rows and {len(df.columns)} columns")
        return df
    except Exception as e:
        logger.error(f"Failed to execute get_program_data function with error --> {e} {traceback.format_exc()}")
        raise


def upload_to_s3(df):
    try:
        logger.info("Executing upload_to_s3 function")
        s3 = S3Operations(logger=logger, profile_name=config["s3_profile"], partition=config["s3_partition"])
        s3.upload_file(
            file_name=config["file_name"],
            file_type=config["file_type"],
            data=df,
            bucket=config["s3_bucket_name"],
            prefix=config["s3_prefix_name"])
        logger.info(
            f"File {config['file_name']} uploaded successfully to {config['s3_bucket_name']}/{config['s3_prefix_name']}")
    except Exception as e:
        logger.error(f"Failed to execute upload_to_s3 function with error --> {e} {traceback.format_exc()}")
        raise


def load_df_to_db(df):
    try:
        logger.info("Starting to load data into the database.")
        df.columns = pd.Series(df.columns).str.lower()
        df = df.applymap(lambda x: str(x) if isinstance(x, dict) else x)
        Database(load_type=config["load_type"], logger=logger, config=config["redshift_config"],
                 profile=config["redshift_profile"], data=df, schema=config["schema_name"],
                 main_table_name=config["main_table_name"])
        logger.info(f"Data loaded successfully into table: {config['main_table_name']}")
    except Exception as e:
        logger.error(f"Failed to execute load_df_to_db function with error --> {e} {traceback.format_exc()}")


def main():
    try:
        logger.info("Main execution started")
        access_token = access_tok(url=config["url_cred"])
        df = get_program_data(api_url=config["url"], token=access_token)
        if not df.empty:
            logger.info("S3 Operations initialized")
            upload_to_s3(df)
            load_df_to_db(df)
            logger.info("Ingestion Completed")
    except Exception as e:
        logger.error(f"Failed to execute main function with error --> {e} {traceback.format_exc()}")


if __name__ == "__main__":
    curt_date = date.today()
    pre_date = date.today() - timedelta(days=3)
    parser = argparse.ArgumentParser(description="Download data and save to a file.")
    parser.add_argument('--infile', nargs=1, help="JSON file with configuration", type=argparse.FileType('r'))
    args = parser.parse_args()
    config = json.load(args.infile[0])
    sys.path.insert(0, config['utils_path'])
    from utils import setup_logger, send_email_notification
    from s3_operations import S3Operations
    from redshift_loader import Database

    log_filename = str(args.infile[0].name).split('/')[-1].replace('.json', '.log')
    log_filename = str(log_filename.replace(".log", f"_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}.log"))
    log_path = os.path.join(config["log_file"], log_filename)
    logger = setup_logger(log_path)
    try:
        logger.info("Ingestion Started")
        main()
################################
import sys
sys.path.append("/home/hvr/Box")
from dbconnections import read_config_file
import glob
import os
import json, csv, sys
from datetime import datetime, timezone, timedelta
import re
import pandas as pd
import io
import requests,json
import configparser
import psycopg2
import sqlalchemy as sa

ARGV=sys.argv
config_path = ARGV[1]
connection_profile = ARGV[2]
filepath=ARGV[3]
schemaname=ARGV[4]
touchfile=ARGV[5]
projectname=ARGV[6]
s3path=ARGV[7]
dl=ARGV[8]

#config_path = "/home/hvr/.aws/redshift_connection.ini" #ARGV[1]
#connection_profile = "Connections_FINNPROD" #ARGV[2]
#filepath="/home/hvr/Box/WDM/data/" #ARGV[3]
#schemaname="hdl_fin_flat_file" #ARGV[4]
#touchfile="/home/hvr/Box/WDM/touchfiles/"#ARGV[5]
#projectname="wdl"
#s3path="s3://odp-fin-nprod-ent-raw/finance_touchfile/"
#dl="503289726@ge.com"#ARGV[6]
cur_date=datetime.now().strftime('%Y_%m_%d-%H_%M')
update_time=datetime.today()
try:
    DBNAME, USER, PASS, HOST, PORT,s3_profile = read_config_file(config_path, connection_profile,dl)
    engine = sa.create_engine(f"postgresql://{USER}:{PASS}@{HOST}:{PORT}/{DBNAME}", echo=True)
    conn = engine.connect()
    conn.autocommit = True
    value=0
    column_lst=["Item","Item_Description","Technical_Nature"]
    for file in os.listdir(filepath):
        try:
            pd.set_option('display.max_colwidth', None)
            df = pd.read_csv(f'{filepath}{file}',quotechar='"',quoting=0,engine='python',encoding='utf-8',names=column_lst)
            df["updated_timestamp"]=update_time
            df = df.where(pd.notnull(df), None)
            file=file.replace(f".csv","")
            file=file.lower()
            exist_query=f"""SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = '{schemaname}' AND tablename = '{file}');"""
            STATUS = engine.execute(exist_query).fetchall()
            if(STATUS[0][0] == True or STATUS[0][0] == 't'):
                truncate_query=f"""truncate table {schemaname}.{file}"""
                engine.execute(truncate_query)
                print(f"the data in the {schemaname}.{file} is truncated ")
            df.to_sql(file,con=conn,schema=schemaname,if_exists='append',index=False,chunksize=500,method='multi')
            value=value+1
            print(f"#####################################{value}")
            os.system(f'touch {touchfile}redshift-{projectname}-{file}-{cur_date}.csv')
            os.system(f'aws s3 cp {touchfile}redshift-{projectname}-{file}-{cur_date}.csv {s3path} --profile {s3_profile}')
        except Exception as e:
            print(e)
            os.system(f'echo "Exception while inserting into the db" | mailx -s "some error occured while executing"{dl}')
except Exception as e:
    print(e)
    os.system(f'echo "Exception while connecting to the database" | mailx -s "some error occured while executing" {dl}')
###########

import sys
sys.path.append("/home/hvr/Box") #to read the db connections
from datetime import datetime, timezone, timedelta
import json
import logging
import boto3
from botocore.exceptions import ClientError
import os
import glob


ARGV=sys.argv
json_file_path= ARGV[1]

with open(f"{json_file_path}", 'r') as j:
    contents = json.loads(j.read())

filepath=contents["path_where_to_load_in_local"] #path where the files are loaded from the box(local)
BUCKET_NAME=contents["bucketname"] #s3 path where the touch file is to be placed in s3 bucket
FOLDER_NAME=contents["folder_structure_in_s3"] # folder path in the s3
dl=contents["DL"] #DL for sending mails

#cur_date=datetime.now().strftime('%Y_%m_%d-%H_%M')
update_time=datetime.today()
try:
    #########################passing columns list ################################
   # column_lst=["Item","Item Description","Technical Nature"]
    #################### reading files from the path where the files are loaded from the box#####################
    files = glob.glob(f'{filepath}*')
    s3_client = boto3.client('s3')
    for filename in files:
        try:
            key = "%s/%s" % (FOLDER_NAME, os.path.basename(filename))
            print("Putting %s as %s" % (filename,key))
            s3_client.upload_file(filename, BUCKET_NAME, key)
        except ClientError as e:
            logging.error(e)
            os.system(f'echo "Exception while connecting to the database" | mailx -s "some error occured while executing" {dl}')
except Exception as e:
    print(e)
    os.system(f'echo "Exception while connecting to the database" | mailx -s "some error occured while executing" {dl}')
=======
{
"configpath":"",
"connectionprofile":"",
"sourcefolderid":"",
"path_where_to_load_in_local":"/home/hvr/Box/Daas/sdaasapi-apicreation/data/",
"Configjson":"/home/hvr/Box_dsp_dataingestion_fin/config/conf.json",
"archievefolderid":"",
"bucketname":"odp-fin-prod-ent-raw",
"folder_structure_in_s3":"sdaasapi-apicreation",
"schemaname":"",
"touchfile":"",
"projectname":"",
"touchfiles3path":"",
"DL":"odpdaas.det@ge.com"
}
]===================================
[hvr@ip-10-229-1-113 ~]$ cat /home/hvr/Box/CTL/Config_json/gl_ctl_pln_targt_s.json
{
    "config_path":"/home/hvr/.aws/redshift_connection.ini",
    "connection_profile":"Connections_FINPROD",
    "filepath" : "/home/hvr/Box/CTL/data/",
    "schemaname":"hdl_fin_ctl_flat_file",
    "columns":
        {
            "GL_CTL_PLN_TARGT_S.csv" : ["Tab", "Category", "Metric", "Attribute", "Value"]
        },
    "touchfile":"/home/hvr/Box/CTL/touchfile/",
    "touchfilename":"gl_ctl_pln_targt_s",
    "projectname":"ctl",
    "s3path":"s3://odp-fin-prod-ent-raw/finance_touchfile/cost_controllership/",
    "dl":"Odpfinancecost@ge.com",
    "logfile":"/home/hvr/Box/CTL/Logs/gl_ctl_pln_targt_s_log.csv",
    "Email_info":
    {
        "failure":
            {
                "subject":"Controllership: API to CTL Flatfile data copy : Prod : Failure",
                "Ingestion_failure":"Exception while inserting into the db. Please refer  to the logfile : ",
                "conn_failure" : "Exception while connecting to the database . please refer to the logfile : "
            }
        }
}

[hvr@ip-10-229-1-113 ~]$ cat /home/hvr/Box/CTL/CTL_Scripts/CTL_TruncateLoad.py
import sys
sys.path.append("/home/hvr/Box")
from dbconnections import read_config_file
import glob
import os,logging
import json, csv, sys
from datetime import datetime, timezone, timedelta
import pandas as pd
import requests,json
import sqlalchemy as sa

ARGV=sys.argv
json_path=ARGV[1]

with open(f"{json_path}")as f:
    data=json.load(f)

config_path = data['config_path']
connection_profile = data['connection_profile']
filepath=data['filepath']
schemaname=data['schemaname']
columns=data['columns']
touchfile=data['touchfile']
touchfilename=data['touchfilename']
projectname=data['projectname']
s3path=data['s3path']
dl=data['dl']
failure=data['Email_info']['failure']
Subject_Failure=failure['subject']
Ingestion_failure=failure['Ingestion_failure']
conn_failure = failure['conn_failure']
logfile=data['logfile']

with open(f'{logfile}', 'a', newline='') as csvfl:
    write_csv = csv.writer(csvfl, delimiter='|')
    write_csv.writerow(['\t TimeStamp \t', 'Severity', '\t Message \t'])

logging.basicConfig(filename=logfile,level=logging.DEBUG, format=('%(asctime)s | %(levelname)s | %(message)s'))
logger = logging.getLogger('hvr')
cur_date=datetime.now().strftime('%Y_%m_%d-%H_%M')

update_time=datetime.today()
try:
    DBNAME, USER, PASS, HOST, PORT,s3_profile = read_config_file(config_path, connection_profile,dl)
    engine = sa.create_engine(f"postgresql://{USER}:{PASS}@{HOST}:{PORT}/{DBNAME}")
    conn = engine.connect()
    logging.info('DB connection is successful')
    conn.autocommit = True
    value=0
    try:
        for file in os.listdir(filepath):
            for key,vals in list(columns.items()):
                if file.__contains__(key):
                    pd.set_option('display.max_colwidth', None)
                    column_lst=columns[file]
                    logging.info(f'current processing file : {file}')
                    df = pd.read_csv(f'{filepath}{file}',encoding='latin1',engine='python',names=column_lst,header=0,index_col=False,dtype=object)
                    df["ingestion_timestamp"]=update_time
                    df = df.where(pd.notnull(df), None)
                    logging.info(f'Total rows and columns in file : {df.shape}')
                    if file=='CTL_RESTRICTED_REQST_NBR_link_to_Box_folder_1049505.csv':
                        file='CTL_RESTRICTED_REQST_NBR.csv'
                    file=file.replace(f".csv","")
                    file=file.lower()
                    exist_query=f"""SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = '{schemaname}' AND tablename = '{file}');"""
                    STATUS = engine.execute(exist_query).fetchall()
                    if(STATUS[0][0] == True or STATUS[0][0] == 't'):
                        truncate_query=f"""truncate table {schemaname}.{file}"""
                        engine.execute(truncate_query)
                        logging.info(f"the data in the {schemaname}.{file} is truncated ")
                    logging.info(f'Data is loading into the table {file}')
                    dataloading=''
                    df.to_sql(file,con=conn,schema=schemaname,if_exists='append',index=False,chunksize=10000,method='multi')
                    dataloading='success'
                    logging.info(f'Data loaded into the table {file}')
                    os.remove(f'{filepath}{key}')      ############ Remove the file from local after ingestion
                    value=value+1
                    print(f"#####################################{value}")
        ###Touch file creating after ingestion of all files
        if dataloading=='success':
            os.system(f'touch  {touchfile}redshift-{projectname}-{touchfilename}-{cur_date}.csv')
            os.system(f'aws s3 cp {touchfile}redshift-{projectname}-{touchfilename}-{cur_date}.csv {s3path} --profile {s3_profile}')
            logging.info(f'Touch file created - {touchfilename}')
    except Exception as e:
        logging.error(f'Exception due to this error : {e}')
        os.system(f'echo "{Ingestion_failure}{logfile}" | mailx -s "{Subject_Failure}"  {dl}')
except Exception as e:
    logging.error(f'Exception due to this error : {e}')
    os.system(f'echo "{conn_failure}{logfile}" | mailx -s "{Subject_Failure}"  {dl}')
########################
import requests,json
import pandas as pd
from sqlalchemy import types
import configparser
import psycopg2
import sqlalchemy as sa
config_path = '/home/hvr/.aws/redshift_connection.ini'
connection_profile = 'Connections_INNOVATION'
# Pick from the following
# ['Connections_PROD', 'Connections_INNOVATION', 'Connections_FINNPROD', 'Connections_FINPROD']
def read_config_file(filepath, connection):
    config = configparser.ConfigParser()
    config.read(filepath)
    db_name = config[connection]['dbname']
    user = config[connection]['user']
    password = config[connection]['password']
    host = config[connection]['host']
    port = int(config[connection]['port'])
    return db_name, user, password, host, port


DBNAME, USER, PASS, HOST, PORT = read_config_file(config_path, connection_profile)
conn = psycopg2.connect(dbname=DBNAME, user=USER, password=PASS, host=HOST, port=PORT)
engine = sa.create_engine(f"postgresql://{USER}:{PASS}@{HOST}:{PORT}/{DBNAME}", echo=True).connect().execution_options(autocommit=True)

#"2912818451244932","INV_SMARTSHEET_OP_ESTMTS_S"

id_lst = ["5502354813413252", "382937121220484", "1285453648291716", "3304683113604996", "1449940292528004", "3474967863027588", "2632588511733636"]
table_lst = ["FRC_SMARTSHEET_CFO_CALL_SUM_NOTES_S", "FRC_SMARTSHEET_GLOBAL_EXEC_REV_COM_S", "FRC_SMARTSHEET_MAKE_CENTER_ROLE_S", "FRC_SMARTSHEET_MODALITY_EXEC_REV_COM_S", "FRC_SMARTSHEET_PLANT_COMMIT_S", "FRC_SMARTSHEET_FINANCE_ESTIMATE_DATE_S", "FRC_SMARTSHEET_FG_OPTION_ENABLED_S"]

#id_lst = ["2912818451244932"]
#table_lst = ["INV_SMARTSHEET_OP_ESTMTS_S"]

token = "2GWV1KIIxRjwDfGC2T413sg1YtMo4KKOtzyeU"
proxy = { "https": "https://http-proxy.health.ge.com:88"}
hedrs = { "Authorization" : f"Bearer {token}", "Accept": "text/csv"}


for id,tablename in zip(id_lst,table_lst):
    url = f"https://api.smartsheet.com/2.0/sheets/{id}"
    response = requests.get(url, headers=hedrs)
    response.raise_for_status()
    sheet = response.content
    sheetname = f"{id}"
    print(sheetname)
    with open(f"/home/hvr/smartsheets_inno_US131322/data/{sheetname}.csv", 'wb+') as f:
        f.write(sheet)
    # conn = psycopg2.connect(dbname='usinnovationredshift', user='502835360', password='iUYp6UAi6JQ8', host='us-innovation-redshift.c8ziwm1qxh67.us-east-1.redshift.amazonaws.com', port=5439)
    Q1= f"""truncate table "{sheetname}" """
    #Q2= f"""truncate table isc_smartsheet_fr."{tablename}" """
    conn = psycopg2.connect(dbname=DBNAME, user=USER, password=PASS, host=HOST, port=PORT)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(Q1)
# engine = create_engine("postgresql://502835360:iUYp6UAi6JQ8@us-innovation-redshift.c8ziwm1qxh67.us-east-1.redshift.amazonaws.com:5439/usinnovationredshift",echo=True)
# conn = engine.connect()
# conn.autocommit = True

for id,tablename in zip(id_lst,table_lst):
    sheetname = f"{id}"
    print(sheetname)
    df = pd.read_csv(f"/home/hvr/smartsheets_inno_US131322/data/{sheetname}.csv",)
    df = df.dropna(how='all')
    df = df.where(pd.notnull(df), None)
    print(df.columns)
    df.to_sql(sheetname,con=engine,if_exists='append',chunksize = 1000,index=False)

for id,tablename in zip(id_lst,table_lst):
    # conn = psycopg2.connect(dbname='usinnovationredshift', user='502835360', password='iUYp6UAi6JQ8', host='us-innovation-redshift.c8ziwm1qxh67.us-east-1.redshift.amazonaws.com', port=5439)
    Q2= f"""truncate table isc_smartsheet_fr."{tablename}" """
    Q1= f"""insert into  isc_smartsheet_fr."{tablename}"  (select * from "{id}") """
    conn = psycopg2.connect(dbname=DBNAME, user=USER, password=PASS, host=HOST, port=PORT)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(Q2)
    cursor.execute(Q1)

##############
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
log_file4 = config["log_file4"]
aws_access_key_id = config['aws_access_key_id']
aws_secret_access_key = config['aws_secret_access_key']
working_directory = config['working_directory']
opr_dl = config['opr_dl']
exclude_tables_lst = config['exclude_tables_lst']
time_stamp_tbls = config['time_stamp_tbls']
tables_lst4 = config["tables_lst4"]
print("list4 tables : ", tables_lst4)
print("Exclude Tables: ", exclude_tables_lst)

with open(log_file4, 'w', newline='') as csv_file:
    write_csv = csv.writer(csv_file, delimiter='|')
    write_csv.writerow(['\t TimeStamp \t', 'Severity', '\t Message \t'])

logging.basicConfig(filename=log_file4,level=logging.INFO, format=('%(asctime)s | %(levelname)s | %(message)s'))
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
        if table_name in tables_lst4:
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
#               os.system(f'echo "This table is failed for this table {table_name} due to this error : {err}" | mailx -s "EHS Sensitive ODP Dev: HVR Ingestion: EHS Sensitive : Failure" {opr_dl}')
                logger.error(traceback.format_exc())
    print("end time:", datetime.today())
    end_dt = datetime.today()
    logger.info(f'endtime : {end_dt}')
    config['failed_tables'] = failed_tables
    with open(f'{working_directory}ingest_parquet.json', 'w') as f:
        f.write(json.dumps(config))
try:
    curt_date = date.today() - timedelta(days=12)
    prefix = curt_date.strftime("%Y-%m-%d")
    print(prefix)
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    lst_objects = s3.list_objects(Bucket=bucket_name, Prefix=prefix)["Contents"]
    total_files = len(lst_objects)
#    os.system(f'echo "Total files available at EHS Sensitive Bucket :{total_files}." | mailx -s "EHS gensuite sensitive_schema ODP Dev: HVR Ingestion: EHS gensuite : Information" {opr_dl}')
    process_parquet_files(lst_objects)
    logger.info(f'Ingestion process started')
    end_time = time.time()
    elapsed_time = (end_time - start_time) / 60
    logger.info('\n Total process duration is ' + str(elapsed_time) + ' mins')

except Exception as err:
    logger.error(traceback.format_exc())
############
from sqlalchemy import create_engine, MetaData, Table, INTEGER, BOOLEAN, TIMESTAMP, DATETIME, DATE, FLOAT, String, types
from sqlalchemy.orm import sessionmaker
import configparser
import pandas as pd
from redshift_connector import get_connection


class Database:
    def __init__(self, logger, config, profile, data, load_type, schema, main_table_name, stage_table_name=None,
                 primary_key=None, log_table_primary_key=None, orderby_col=None, log_table=None):

        self.logger = logger
        self.logger.info("Running Database Module")
        self.main_table = main_table_name
        self.stage_table = stage_table_name
        self.data = data
        self.primary_key = primary_key
        self.orderby_col = orderby_col
        self.load_type = load_type
        self.engine = get_connection(config, profile, logger)
        self.metadata = MetaData(bind=self.engine, schema=schema)
        self.Session = sessionmaker(bind=self.engine)
        self.schema = schema
        self.log_table_primary_key = log_table_primary_key
        self.log_table = log_table
        self.initiate_load()
        self.close()

    def initiate_load(self):
        try:
            if self.load_type == "truncate_and_load":
                self.logger.info("Proceeding with truncate and load")
                self.truncate_table(self.main_table)
                self.insert_data(self.main_table, self.data)
            elif self.load_type == "fullload":
                self.logger.info("Proceeding with append only")
                self.insert_data(self.main_table, self.data)
            elif self.load_type == "incremental":
                self.logger.info("Proceeding with incremental load")
                self.truncate_table(self.stage_table)
                self.insert_data(self.stage_table, self.data)
                self.incremental_load(self.main_table, self.stage_table, self.primary_key)
            elif self.load_type == "remove_duplicates_and_load":
                self.logger.info("Proceeding with remove duplicate_and_load load load to main table")
                self.drop_duplicates(self.stage_table, self.primary_key, self.orderby_col)
                self.incremental_load(self.main_table, self.stage_table, self.primary_key)
            elif self.load_type == "log_based_soft_deletes":
                self.logger.info("Proceeding with soft_deletes load")
                self.log_based_soft_deletes(self.main_table, self.log_table, self.primary_key,
                                            self.log_table_primary_key, self.schema)
        except Exception as e:
            self.logger.error(f"initiate_load method execution failed with error --> {e}")
            raise

    def transform(self, data, table):
        """
        A method to perform Data Type transformations if not matched with DataFrame and table created in DB

        Parameters:
        data (DataFrame) : Pandas DataFrame constructed from any type (csv,excel,parquet,text)
        table (object)   : Object of the table

        Returns:
        data (DataFrame) : Pandas DataFrame which has undergone Data Type casting
        """
        self.logger.info(
            f"Executing transform method in Database class to perform type casting if required for {table}")
        try:
            df_dtypes = data.dtypes.to_dict()
            table_dtypes = {col.name: col.type for col in table.columns}
            sqlalchemy_to_pandas_dtype = {
                INTEGER: 'int64',
                BOOLEAN: 'bool',
                TIMESTAMP: 'datetime64[ns]',
                DATETIME: 'datetime64[ns]',
                DATE: 'datetime64[ns]',
                FLOAT: 'float64',
                String: 'object'

            }
            for col_name, sqlalchemy_type in table_dtypes.items():
                expected_dtype = sqlalchemy_to_pandas_dtype.get(type(sqlalchemy_type), 'object')
                if expected_dtype and df_dtypes[col_name] != expected_dtype:
                    data[col_name] = data[col_name].astype(expected_dtype)
            self.logger.info(
                f"transform method executed successfully - Returning data by checking for data types after doing any casting if required")
            return data
        except Exception as e:
            self.logger.error(
                f"Failed to execute transform method in Database class. Type casting failed, error --> {e}")
            raise

    def close(self):
        """
        A method to close the DB connection

        Parameters : None
        Returns : None
        """
        self.logger.info("Closing DB connection by executing close method in Database class")
        try:
            self.Session.close_all()
            self.engine.close()
            self.logger.info("DB Connections closed")
        except Exception as e:
            self.logger.error(
                f"Failed to execute close method in Database class. DB connection was not closed, error --> {e}")
            raise

    def truncate_table(self, table_name: str):
        self.logger.info(f"Executing truncate_table method in Database class for {table_name}")
        try:
            table = Table(table_name, self.metadata, autoload_with=self.engine)
            with self.Session() as session:
                session.execute(table.delete())
                self.logger.info(f"{table_name} has been truncated")
                session.commit()
                self.logger.info("Truncate operation has been committed")
                self.logger.info(f"truncate_table method executed successfully - {table_name} has been truncated")
        except Exception as e:
            self.logger.error(
                f"Failed to execute truncate_table method in Database class for {table_name}, error --> {e}")
            raise

    @staticmethod
    def sqlcol(dfparam):
        """
        A static method to enable collation and input object as varchar values while performing db insert

        Parameters:
        dfparam (DataFrame) : DataFrame which has to undergo this change and will be inserted to DB

        Returns : None
        """
        dtypedict = {}
        for i, j in zip(dfparam.columns, dfparam.dtypes):
            if "object" in str(j):
                dtypedict.update({i: types.VARCHAR(collation='case_insensitive')})
        return dtypedict

    def insert_data(self, table_name, data):
        self.logger.info(f"Executing insert_data method in Database class for {table_name}")
        try:
            table = Table(table_name, self.metadata, autoload_with=self.engine)
            transformed_data = self.transform(data, table)
            transformed_data.to_sql(name=table_name, schema=self.schema, con=self.engine, if_exists='append',
                                    index=False, chunksize=10000, method='multi', dtype=self.sqlcol(transformed_data))
            self.logger.info(f"Insert into {table_name} completed - insert_data method executed successfully")
        except Exception as e:
            self.logger.error(f"Failed to execute insert_data method in Database class for {table_name}, error --> {e}")
            raise

    def incremental_load(self, main_table, stage_table, primary_key):
        """
        A method to execute upsert/incremental operations on the tables provided as input

        Parameters:
        main_table (str)  : Name of the main table to which incremental records have to be inserted
        stage_table (str) : Name of the table which has incremental data pulled from source
        primary_key (str) : Primary Key column which is present in both main_table and stage_table

        Returns : None
        """
        self.logger.info(
            f"Executing incremental_load method in Database class for main table {main_table} and stage table {stage_table}")
        try:
            delete_query = f"""
            DELETE FROM {self.schema}.{main_table} USING {self.schema}.{stage_table}
            WHERE {self.schema}.{main_table}.{primary_key} = {self.schema}.{stage_table}.{primary_key}
            """
            insert_query = f"""
            INSERT INTO {self.schema}.{main_table}
            SELECT * FROM {self.schema}.{stage_table}
            """
            self.engine.execute(delete_query)
            self.logger.info(
                f"Deleted records in main table {main_table} matching with the ones in stage table {stage_table}")
            self.engine.execute(insert_query)
            self.logger.info(f"Inserted incremental records in main table {main_table}")
            self.logger.info(f"incremental_load method executed successfully for {stage_table} and {main_table}")
        except Exception as e:
            self.logger.error(
                f"Failed to execute incremental_load method in Database class for main table {main_table} & stage table {stage_table}, error --> {e}")
            raise

    def drop_duplicates(self, stage_table, primary_key, orderby_col):
        try:
            self.logger.info(f"Executing drop_duplicates method in Database class for stage table {stage_table}")
            create_temp_table_query = f"""
            create table {self.schema}.{stage_table}_temp as
            SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY {primary_key} ORDER BY {orderby_col} DESC) AS row_num
            FROM {self.schema}.{stage_table})
            """
            duplicate_delete_query = f"""
            DELETE FROM {self.schema}.{stage_table}_temp
            where row_num > 1
            """
            drop_row_num_column_query = f"""
            Alter table {self.schema}.{stage_table}_temp drop column row_num
            """
            drop_and_rename_query = f"""
            drop table {self.schema}.{stage_table};
            Alter table {self.schema}.{stage_table}_temp rename to {stage_table}
            """
            self.engine.execute(create_temp_table_query)
            self.logger.info(f"Created an {stage_table}_temp with row number column")
            r = self.engine.execute(duplicate_delete_query)
            self.logger.info(f"No of Duplicates removed : {r.rowcount}")
            self.logger.info(f"Removed the duplicates from {stage_table}_temp table")
            self.engine.execute(drop_row_num_column_query)
            self.logger.info(f"Dropped the row number column from {stage_table}_temp table")
            self.engine.execute(drop_and_rename_query)
            self.logger.info(f"Rename the {stage_table}_temp to {stage_table}")
        except Exception as e:
            self.logger.error(
                f"Failed to execute drop_duplicates method in Database class for stage table {stage_table}, error --> {e}")
            raise

    def log_based_soft_deletes(self, main_table, log_table, primary_key, log_table_primary_key, schema):
        """
        A method is to handle the soft deletes on main table with refrence of the log table

        Parameters:

        main_table(str) : Name of the table to handle soft deletes
        log_table(str) : Name of the log table that refers main table to handle soft deletes
        primary_key(str) : Primary key column from main table
        log_table_primary_key(str) : Primary key column from log table
        Schema(str) : Schema name of the tables
        """
        self.logger.info(f"Executing soft_deletes method for main table {main_table}")
        try:
            soft_deletes_query = f"""
                UPDATE {schema}.{main_table}
                SET is_deleted = 1
                WHERE is_deleted = 0
                AND EXISTS (
                   SELECT 1
                   FROM {schema}.{log_table} log
                   WHERE log.{log_table_primary_key} = {schema}.{main_table}.{primary_key})"""
            self.logger.info(f"Soft_delete script executed sucessfully for {main_table}")
            results = self.engine.execute(soft_deletes_query)
            no_rows_updated = results.rowcount
            self.logger.info(f"Toatal no of records has been processed : {no_rows_updated}")
            print(f"Toatal no of records has been processed : {no_rows_updated}")

        except Exception as e:
            self.logger.info(
                f"failed to exceute soft_deletes method in Database class for main table {main_table}, error -->{e}")
            raise

#################


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
##############
[hvr@ip-10-242-109-196 config]$ cat /home/hvr/box_ops/getfilesfrombox.py

from datetime import datetime, timezone, timedelta
import glob
import os
from boxsdk import JWTAuth, Client
import json, csv, sys
import re
import io
import requests,json
import argparse


parser = argparse.ArgumentParser(description='Azure Storage to S3.')
parser.add_argument('--configpath')

args = parser.parse_args()
json_file_path = args.configpath

with open(f"{json_file_path}", 'r') as j:
    contents = json.loads(j.read())

cur_date=datetime.now().strftime('%Y_%m_%d:%H_%M')

try:
    #################getting access from the json file######################
    def getAccessToBox(JWT_file_path):
        auth = JWTAuth.from_settings_file(JWT_file_path)
        return Client(auth)

    #########################changing the file names in correct formt############
    def getFileWithChangeName(client,file_id,dest_location,box_folder_id):
        file_name = client.file(file_id).get().name.replace(' ', '_')
        #print(file_name)
        #if file_name == "Valuetrack_Extract.csv":
        open_file=open((os.path.join(dest_location,file_name)), 'wb')
        client.file(file_id).download_to(open_file)
        open_file.close()

    ################get files to the local #########################
    def getFiles(client,folder_id,Files_Stage_location):
        filesLoaded=list()
        for item in client.folder(folder_id).get_items():
            if item.type == "folder":
                print(f"{item} is a folder")
            elif item.type == "file":
                filename=client.file(item.id).get().name
                if filename.__contains__(".csv"):
                    print(filename)
                    getFileWithChangeName(client,item.id,Files_Stage_location,folder_id)
                    filesLoaded.append(item)
                   # move_to_archive(item)
        return filesLoaded

    #############moving files to archieve location in box################
   # def move_to_archive(item):
        #file_to_move = client.file(item.id).rename(f"_{cur_date}.".join(item.name.split('.')))
        #destination_folder_id = contents["archievefolderid"]#'206172309808'
        #destination_folder = client.folder(destination_folder_id)
        #moved_file = file_to_move.move(parent_folder=destination_folder)
        #print(f"file{file_to_move} moved to archieved loaction")

    ########################main method #################################
    if __name__ == "__main__":
        folder = contents["sourcefolderid"]#folder id
        filepath = contents["path_where_to_load_in_local"]
        client = getAccessToBox(f'{contents["Configjson"]}')
        getFiles(client,folder,filepath)
except Exception as e:
    print(e)
    print("some error occured while executing")
    os.system(f'echo "Exception occured while getting files from box" | mailx -s "some error occured while executing" {contents["DL"]}')
===============
{
"connfigpath":"/home/hvr/.aws/redshift_connection.ini",
"connectionprofile":"Connections_PROD",
"sourcefolderid":"226296292388",
"path_where_to_load_in_local":"/home/hvr/box_ops/PDx/DSI_China/data/",
"Configjson":"/home/hvr/.aws/conf.json",
"archievefolderid":"",
"bucketname":"odp-us-prod-hc-pdx",
"folder_structure_in_s3":"Ingestion/DSI China",
"schemaname":"",
"touchfile":"",
"projectname":"",
"touchfiles3path":"",
"DL":"",
"incremental_lst" : [""]
}
========================================================================
import sys
sys.path.append("/home/hvr/Box") #to read the db connections
from datetime import datetime, timezone, timedelta
import json
import logging
import boto3
from botocore.exceptions import ClientError
import os
import glob


ARGV=sys.argv
json_file_path= ARGV[1]

with open(f"{json_file_path}", 'r') as j:
    contents = json.loads(j.read())

filepath=contents["path_where_to_load_in_local"] #path where the files are loaded from the box(local)
BUCKET_NAME=contents["bucketname"] #s3 path where the touch file is to be placed in s3 bucket
FOLDER_NAME=contents["folder_structure_in_s3"] # folder path in the s3
dl=contents["DL"] #DL for sending mails
incremental_lst = contents["incremental_lst"]
#cur_date=datetime.now().strftime('%Y_%m_%d-%H_%M')
update_time=datetime.today()
try:
    #########################passing columns list ################################
   # column_lst=["Item","Item Description","Technical Nature"]
    #################### reading files from the path where the files are loaded from the box#####################
    files = glob.glob(f'{filepath}*')
    s3_client = boto3.client('s3')
    for filename in files:
        try:
            dt = datetime.now()
            year, month, day, hour = dt.strftime('%Y'), dt.strftime('%m'), dt.strftime('%d'),dt.strftime('%H')
            print(filename,'filename')
            if os.path.basename(filename) in incremental_lst:
                key ="%s/%s/%s/%s/%s/%s" % (FOLDER_NAME,year, month, day, hour, os.path.basename(filename))
            else:
                key = "%s/%s" % (FOLDER_NAME, os.path.basename(filename))
            #key = "%s/%s/%s/%s/%s/%s" % (FOLDER_NAME,year, month, day, hour, os.path.basename(filename))
            #key = "%s/%s" % (FOLDER_NAME, os.path.basename(filename))
            print("Putting %s as %s" % (filename,key))
            #print("BUCKET_NAME",BUCKET_NAME)
            s3_client.upload_file(filename, BUCKET_NAME, key)
        except ClientError as e:
            logging.error(e)
            os.system(f'echo "Exception while connecting to the database" | mailx -s "some error occured while executing" {dl}')
except Exception as e:
    print(e)
    os.system(f'echo "Exception while connecting to the database" | mailx -s "some error occured while executing" {dl}')
'''{
"connfigpath":"/home/hvr/.aws/redshift_connection.ini",
"connectionprofile":"Connections_PROD",
"sourcefolderid":"226296292388",
"path_where_to_load_in_local":"/home/hvr/box_ops/PDx/DSI_China/data/",
"Configjson":"/home/hvr/.aws/conf.json",
"archievefolderid":"",
"bucketname":"odp-us-prod-hc-pdx",
"folder_structure_in_s3":"Ingestion/DSI China",
"schemaname":"",
"touchfile":"",
"projectname":"",
"touchfiles3path":"",
"DL":"",
"incremental_lst" : [""]
}
'''
##########################

from sqlalchemy import create_engine, MetaData, Table, INTEGER, BOOLEAN, TIMESTAMP, DATETIME, DATE, FLOAT,String,types
from sqlalchemy.orm import sessionmaker
import configparser
import pandas as pd
from redshift_connector import get_connection

class Database:
    """
    A class Database which performs Truncate, Insert, and Upsert/Incremental operations on the provided tables
    """
    def __init__(self, logger, config, profile, data, load_type, schema, main_table_name, stage_table_name=None, primary_key=None,log_table_primary_key=None,orderby_col=None,log_table=None):
        """
        The constructor for Database class

        Parameters:
        logger (object)        : Logger object where log entries are to be made
        config (str)           : Path of redshift credentials
        profile (str)          : Redshift profile
        data (DataFrame)       : DataFrame constructed from the response fetched
        load_type (str)        : truncate_and_load / incremental / fullload - input based on the requirement
        schema (str)           : Schema name
        main_table_name (str)  : Main target table name
        stage_table_name (str) : Target Stage table name required if incremental load type or by default it is None
        primary_key (str)      : Primary Key in the table if incremental load type or by default it is None
        orderby_col (str)      : orderby_col in the table if remove_duplicates_and_load load type or by default it is None
        log_table_primary_key(str) : Primark key in the log table if soft_deletes load type or by default it is None
        log_table (str)        : log table name
        """
        self.logger = logger
        self.logger.info("Running Database Module")
        self.main_table = main_table_name
        self.stage_table = stage_table_name
        self.data = data
        self.primary_key = primary_key
        self.orderby_col = orderby_col
        self.load_type = load_type
        self.engine = get_connection(config, profile, logger)
        self.metadata = MetaData(bind=self.engine, schema=schema)
        self.Session = sessionmaker(bind=self.engine)
        self.schema=schema
        self.log_table_primary_key = log_table_primary_key
        self.log_table = log_table
        self.initiate_load()
        self.close()

    def initiate_load(self):
        """
        A method to initiate type of load method based on the inputs received

        Parameters: None
        Returns: None
        """
        try:
            if self.load_type == "truncate_and_load":
                self.logger.info("Proceeding with truncate and load")
                self.truncate_table(self.main_table)
                self.insert_data(self.main_table, self.data)
            elif self.load_type == "fullload":
                self.logger.info("Proceeding with append only")
                self.insert_data(self.main_table, self.data)
            elif self.load_type == "incremental":
                self.logger.info("Proceeding with incremental load")
                self.truncate_table(self.stage_table)
                self.insert_data(self.stage_table, self.data)
                self.incremental_load(self.main_table, self.stage_table, self.primary_key)
            elif self.load_type == "remove_duplicates_and_load":
                self.logger.info("Proceeding with remove duplicate_and_load load load to main table")
                self.drop_duplicates(self.stage_table,self.primary_key,self.orderby_col)
                self.incremental_load(self.main_table, self.stage_table, self.primary_key)
            elif self.load_type =="log_based_soft_deletes":
                self.logger.info("Proceeding with soft_deletes load")
                self.log_based_soft_deletes(self.main_table, self.log_table, self.primary_key, self.log_table_primary_key, self.schema)
        except Exception as e:
            self.logger.error(f"initiate_load method execution failed with error --> {e}")
            raise

    def transform(self, data, table):
        """
        A method to perform Data Type transformations if not matched with DataFrame and table created in DB

        Parameters:
        data (DataFrame) : Pandas DataFrame constructed from any type (csv,excel,parquet,text)
        table (object)   : Object of the table

        Returns:
        data (DataFrame) : Pandas DataFrame which has undergone Data Type casting
        """
        self.logger.info(f"Executing transform method in Database class to perform type casting if required for {table}")
        try:
            df_dtypes = data.dtypes.to_dict()
            table_dtypes = {col.name: col.type for col in table.columns}
            sqlalchemy_to_pandas_dtype = {
                INTEGER: 'int64',
                BOOLEAN: 'bool',
                TIMESTAMP: 'datetime64[ns]',
                DATETIME: 'datetime64[ns]',
                DATE: 'datetime64[ns]',
                FLOAT: 'float64',
                String : 'object'

            }
            for col_name, sqlalchemy_type in table_dtypes.items():
                expected_dtype = sqlalchemy_to_pandas_dtype.get(type(sqlalchemy_type), 'object')
                if expected_dtype and df_dtypes[col_name] != expected_dtype:
                    data[col_name] = data[col_name].astype(expected_dtype)
            self.logger.info(f"transform method executed successfully - Returning data by checking for data types after doing any casting if required")
            return data
        except Exception as e:
            self.logger.error(f"Failed to execute transform method in Database class. Type casting failed, error --> {e}")
            raise

    def close(self):
        """
        A method to close the DB connection

        Parameters : None
        Returns : None
        """
        self.logger.info("Closing DB connection by executing close method in Database class")
        try:
            self.Session.close_all()
            self.engine.close()
            self.logger.info("DB Connections closed")
        except Exception as e:
            self.logger.error(f"Failed to execute close method in Database class. DB connection was not closed, error --> {e}")
            raise

    def truncate_table(self, table_name: str):
        """
        A method to execute truncate operation on the table provided as input

        Parameters:
        table_name (str) : Name of the table which data has to be truncated

        Returns : None
        """
        self.logger.info(f"Executing truncate_table method in Database class for {table_name}")
        try:
            table = Table(table_name, self.metadata, autoload_with=self.engine)
            with self.Session() as session:
                session.execute(table.delete())
                self.logger.info(f"{table_name} has been truncated")
                session.commit()
                self.logger.info("Truncate operation has been committed")
                self.logger.info(f"truncate_table method executed successfully - {table_name} has been truncated")
        except Exception as e:
            self.logger.error(f"Failed to execute truncate_table method in Database class for {table_name}, error --> {e}")
            raise

    @staticmethod
    def sqlcol(dfparam):
        """
        A static method to enable collation and input object as varchar values while performing db insert

        Parameters:
        dfparam (DataFrame) : DataFrame which has to undergo this change and will be inserted to DB

        Returns : None
        """
        dtypedict = {}
        for i, j in zip(dfparam.columns, dfparam.dtypes):
            if "object" in str(j):
                dtypedict.update({i: types.VARCHAR(collation='case_insensitive')})
        return dtypedict

    def insert_data(self, table_name, data):
        """
        A method to execute insert operation on the table provided as input with data

        Parameters:
        table_name (str) : Name of the table which data has to be inserted
        data (DataFrame) : Pandas DataFrame constructed from any type (csv,excel,parquet,text)

        Returns : None
        """
        self.logger.info(f"Executing insert_data method in Database class for {table_name}")
        try:
            table = Table(table_name, self.metadata, autoload_with=self.engine)
            transformed_data = self.transform(data, table)
            transformed_data.to_sql(name=table_name, schema=self.schema,con=self.engine, if_exists='append', index=False, chunksize=10000,method='multi',dtype=self.sqlcol(transformed_data))
            self.logger.info(f"Insert into {table_name} completed - insert_data method executed successfully")
        except Exception as e:
            self.logger.error(f"Failed to execute insert_data method in Database class for {table_name}, error --> {e}")
            raise

    def incremental_load(self, main_table, stage_table, primary_key):
        """
        A method to execute upsert/incremental operations on the tables provided as input

        Parameters:
        main_table (str)  : Name of the main table to which incremental records have to be inserted
        stage_table (str) : Name of the table which has incremental data pulled from source
        primary_key (str) : Primary Key column which is present in both main_table and stage_table

        Returns : None
        """
        self.logger.info(f"Executing incremental_load method in Database class for main table {main_table} and stage table {stage_table}")
        try:
            delete_query = f"""
            DELETE FROM {self.schema}.{main_table} USING {self.schema}.{stage_table}
            WHERE {self.schema}.{main_table}.{primary_key} = {self.schema}.{stage_table}.{primary_key}
            """
            insert_query = f"""
            INSERT INTO {self.schema}.{main_table}
            SELECT * FROM {self.schema}.{stage_table}
            """
            self.engine.execute(delete_query)
            self.logger.info(f"Deleted records in main table {main_table} matching with the ones in stage table {stage_table}")
            self.engine.execute(insert_query)
            self.logger.info(f"Inserted incremental records in main table {main_table}")
            self.logger.info(f"incremental_load method executed successfully for {stage_table} and {main_table}")
        except Exception as e:
            self.logger.error(f"Failed to execute incremental_load method in Database class for main table {main_table} & stage table {stage_table}, error --> {e}")
            raise

    def drop_duplicates(self, stage_table, primary_key, orderby_col) :
        try :
            self.logger.info(f"Executing drop_duplicates method in Database class for stage table {stage_table}")
            create_temp_table_query = f"""
            create table {self.schema}.{stage_table}_temp as
            SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY {primary_key} ORDER BY {orderby_col} DESC) AS row_num
            FROM {self.schema}.{stage_table})
            """
            duplicate_delete_query = f"""
            DELETE FROM {self.schema}.{stage_table}_temp
            where row_num > 1
            """
            drop_row_num_column_query = f"""
            Alter table {self.schema}.{stage_table}_temp drop column row_num
            """
            drop_and_rename_query = f"""
            drop table {self.schema}.{stage_table};
            Alter table {self.schema}.{stage_table}_temp rename to {stage_table}
            """
            self.engine.execute(create_temp_table_query)
            self.logger.info(f"Created an {stage_table}_temp with row number column")
            r=self.engine.execute(duplicate_delete_query)
            self.logger.info(f"No of Duplicates removed : {r.rowcount}")
            self.logger.info(f"Removed the duplicates from {stage_table}_temp table")
            self.engine.execute(drop_row_num_column_query)
            self.logger.info(f"Dropped the row number column from {stage_table}_temp table")
            self.engine.execute(drop_and_rename_query)
            self.logger.info(f"Rename the {stage_table}_temp to {stage_table}")
        except Exception as e:
            self.logger.error(f"Failed to execute drop_duplicates method in Database class for stage table {stage_table}, error --> {e}")
            raise

    def log_based_soft_deletes(self,main_table,log_table,primary_key,log_table_primary_key,schema):
        """
        A method is to handle the soft deletes on main table with refrence of the log table

        Parameters:

        main_table(str) : Name of the table to handle soft deletes
        log_table(str) : Name of the log table that refers main table to handle soft deletes
        primary_key(str) : Primary key column from main table
        log_table_primary_key(str) : Primary key column from log table
        Schema(str) : Schema name of the tables
        """
        self.logger.info(f"Executing soft_deletes method for main table {main_table}")
        try:
            soft_deletes_query = f"""
                UPDATE {schema}.{main_table}
                SET is_deleted = 1
                WHERE is_deleted = 0
                AND EXISTS (
                   SELECT 1
                   FROM {schema}.{log_table} log
                   WHERE log.{log_table_primary_key} = {schema}.{main_table}.{primary_key})"""
            self.logger.info(f"Soft_delete script executed sucessfully for {main_table}")
            results = self.engine.execute(soft_deletes_query)
            no_rows_updated = results.rowcount
            self.logger.info(f"Toatal no of records has been processed : {no_rows_updated}")
            print(f"Toatal no of records has been processed : {no_rows_updated}")

        except Exception as e:
            self.logger.info(f"failed to exceute soft_deletes method in Database class for main table {main_table}, error -->{e}")
            raise



