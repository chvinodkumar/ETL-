import json
import logging
import requests
import pandas as pd
import platform
from flatten_json import flatten
from sqlalchemy import types, create_engine
import configparser
from datetime import datetime
from utils import get_connection, setup_logger, send_email_notification
import os

class GraphqlTable:
    def __init__(self, config, logger:logging) -> None:
        self.logger = logger
        self.config = config
        self.header = {}
        self.hasNextPage = True
        self.total_count = 0
        self.nextPage = ""
        self.data = []
        self.columns = {}
        self.__raw_data = {}
        self.__read_config()
        self.__prerequisites()
        self.get_gql_data()
        self.connection = get_connection(self.config['config_path'], self.config['connection_profile'])

    def __read_config(self):
        # config = json.load(self.config_file) if type(self.config_file)!=str else json.load(open(self.config_file))
        # self.config = config
        self.columns.update(self.extreact_columns(config['columns'], 'name'))
        # self.logger.info(f"{logger.name} {logger.hasHandlers} {dir(logger)}")
        # exit()

    def __prerequisites(self):
        auth_res = requests.post(self.config["auth_url"], auth=('apitoken', self.config['api_token']), proxies=self.config['proxies']  if platform.system()=='Windows' else None, verify=False,  data={'grant_type': 'client_credentials'})
        try:
            auth_res.raise_for_status()
        except requests.exceptions.HTTPError as e:
            # self.logger.error(f"Authentication failed")
            self.logger.error(f"{e}")
        access_token = auth_res.json()['access_token']
        auth_header = 'Bearer ' + access_token
        self.header = {'Authorization': auth_header}
        self.logger.info(f"Api Authentication Sucessful")

    def parse_raw_data(self):
        self.total_count =  self.__raw_data['totalCount']
        self.hasNextPage =  self.__raw_data['pageInfo']['hasNextPage']
        self.nextPage =  self.__raw_data['pageInfo']['endCursor']
        for i in range(len(self.__raw_data['edges'])):
            self.data.append({**self.__raw_data['edges'][i]['node']})

    def get_gql_data(self):
        self.logger.info("Capturing data from Graphql URL")
        while self.hasNextPage:
            data = {"query": self.config["query"], 'variables': {"after": self.nextPage}}
            json_data = json.dumps(data)
            response = requests.post(url=self.config["request_url"], proxies=self.config['proxies']  if platform.system()=='Windows' else None, verify=False, headers=self.header, data=json_data)
            self.__raw_data = response.json()['data']['allFactSheets']
            self.parse_raw_data()
        self.logger.info("capture completed")

    def extreact_columns(self, raw_clms, key):
        clms = {}
        for clm in raw_clms.keys():
            clms.update({clm:raw_clms[clm][key]})
        return clms

    def sqlcol(self, dfparam:pd.DataFrame):
        dtypedict = {}
        for i, j in zip(dfparam.columns, dfparam.dtypes):
            if "object"in str(j):
                if dfparam[i].str.len().max() >= 250:
                    dtypedict.update({i: types.VARCHAR(65000, collation='case_insensitive')})
                else:
                    dtypedict.update({i: types.VARCHAR(256, collation='case_insensitive')})
        return dtypedict



if __name__=="__main__":
    # Reading config file
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--infile', nargs=1, help="JSON file to be processed", type=argparse.FileType('r'))
    arguments = parser.parse_args()
    config = json.load(arguments.infile[0])

    # Setting up logging
    parent_path = os.path.dirname(os.path.abspath(__file__))
    log_filename = str(arguments.infile[0].name).split('/')[-1].replace('json', 'log')
    logger = setup_logger(os.path.join(parent_path, "logs", log_filename))

    try:
        gql_data = GraphqlTable(config, logger)
        logger.info(f"Ingestion for {gql_data.config['table_name']} table started")
        if gql_data.config["table_name"]=="hc_dtb_leanix_app_dept_org_f":
            def custom_flatten(sub_data):
                temp_sub_data = {}
                temp_lc_data = {}
                for i in sub_data['subscriptions']['edges']:
                    temp_sub_data.update({str(i['node']['roles'][0]['name']).replace(" ", "_"):i['node']['user']['email']})
                if sub_data['lifecycle'] != None:
                    for j in sub_data['lifecycle']['phases']:
                        temp_lc_data.update({'CurrentState': sub_data['lifecycle']['CurrentState'], j['Stage']:j['Date']})
                return {**sub_data, "subscriptions": temp_sub_data, 'lifecycle':temp_lc_data}
            data = [flatten(custom_flatten(d)) for d in gql_data.data]
        else:
            data = [flatten(i) for i in gql_data.data]
        df = pd.DataFrame(data).fillna('')
        df =df[gql_data.columns.keys()]
        df = df.rename(columns=gql_data.columns).astype(str)
        logger.info(f"loading {df.shape[0]} records & {df.shape[1]} columns to redshift ...")
        df['data_origin'] = 'leanix'
        df['posting_agent'] = 'leanix_ingestion.py'
        df['load_dtm'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df.to_sql(gql_data.config["table_name"], con=gql_data.connection, index=False, schema=gql_data.config["schema_name"], dtype=gql_data.sqlcol(df), if_exists='replace', method='multi', chunksize=1000)
        logger.info(f"Ingestion for {gql_data.config['table_name']} table finished")
    except Exception as err:
        logger.error(f"error: {err}")
        logger.error(f"Ingestion failed with above error")
        send_email_notification("Leanix Ingestion failed", err, config['email_stack_holders'])
