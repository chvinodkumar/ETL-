import os
import json
import requests
from datetime import datetime
import numpy as np
import urllib3
import pandas as pd
import threading
import pytz
from utils import get_connection, send_email_notification, setup_logger

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
    return res.json()["hits"]["hits"]

def get_related_cis(uai):
    body = {
    "size": 1000,
    "query": {
        "bool": {
            "must": [
                {
                    "nested": {
                        "path": "app_relationships",
                        "query": {
                            "match": {
                                "app_relationships.u_unique_app_id": "{}".format(uai)
                            }
                        }
                    }
                },
                {
                    "match": {
                        "company": "GE Healthcare"
                        }
                    }
                ]
            }
        }
    }
    return search_request(body)

def crawlAndGetData(item_name,hit):
    try:
        item = hit[item_name].encode('utf-8').strip()
        return item.decode('utf-8')
    except Exception as e:
        return None

def extract_cis(app_results,data_list,size,thread_num):
    for i, app in enumerate(app_results):
        app_id = app["u_unique_app_id"]

        app_child_results = [a["_source"] for a in get_related_cis(app_id)]

        for j,app_child in enumerate(app_child_results):
            ls =[
            app_id,
            crawlAndGetData('u_config_item_id',app_child),
            crawlAndGetData('u_config_item_id',app),
            crawlAndGetData('sys_class_name',app_child),
            crawlAndGetData('sys_class_name',app),
            crawlAndGetData('name',app_child),
            crawlAndGetData('name',app),
            crawlAndGetData('support_group',app_child),
            crawlAndGetData('support_group',app),
            crawlAndGetData('owner_name',app_child),
            crawlAndGetData('owner_name',app),
            crawlAndGetData('owner_sso',app_child),#newly added
            crawlAndGetData('owner_sso',app),#newly added
            crawlAndGetData('operational_status',app_child),
            crawlAndGetData('operational_status',app),
            crawlAndGetData('model_classification',app_child),
            crawlAndGetData('model_classification',app),
            crawlAndGetData('model',app_child),#newly added
            crawlAndGetData('install_status',app_child),
            crawlAndGetData('install_status',app),
            crawlAndGetData('install_date',app_child),
            crawlAndGetData('install_date',app),
            crawlAndGetData('u_uninstall_date',app_child),
            crawlAndGetData('u_uninstall_date',app),
            crawlAndGetData('actual_retirement',app_child),#newly added
            crawlAndGetData('department',app_child),
            crawlAndGetData('department',app),
            crawlAndGetData('u_business_unit',app_child),#newly added
            crawlAndGetData('u_business_unit',app),
            crawlAndGetData('u_business_segment',app_child),#newly added
            crawlAndGetData('u_business_segment',app),
            crawlAndGetData('company',app_child),
            crawlAndGetData('company',app),
            crawlAndGetData('location_name',app_child),#newly added
            crawlAndGetData('u_functional_owner',app_child),
            crawlAndGetData('u_functional_owner',app),
            crawlAndGetData('gis_support_class',app_child),
            crawlAndGetData('gis_support_class',app),
            crawlAndGetData('used_for',app_child),
            crawlAndGetData('used_for',app),
            crawlAndGetData('ip_address',app_child),
            crawlAndGetData('max_app_business_criticality',app_child),
            crawlAndGetData('max_app_business_criticality',app),
            crawlAndGetData('u_classification',app_child),
            crawlAndGetData('u_classification',app),
            crawlAndGetData('max_app_environment',app),
            crawlAndGetData('max_app_environment',app_child),
            crawlAndGetData('serial_number',app_child),
            crawlAndGetData('u_location_code',app_child),
            crawlAndGetData('u_location_code',app),
            crawlAndGetData('country',app_child),
            crawlAndGetData('country',app),
            crawlAndGetData('u_region',app_child),
            crawlAndGetData('u_region',app),
            crawlAndGetData('city',app_child),
            crawlAndGetData('city',app),
            str(dt)
            ]
            data_list.append(ls)

def get_buss_app(compny):
    body = {
        "size": 1000,
        "query": {
            "bool": {
                "must": [
                    {"match": {"sys_class_name": "u_cmdb_ci_business_app"}},
                    {"match": {"company": "{}".format(compny)}},
                ]
            }
        },
    }
    return search_request(body)

def get_other_buss_app():
    body = {
    "query": {
        "bool": {
            "must": {
                "match": {
                    "sys_class_name": "u_cmdb_ci_business_app"
                }
            },
            "must_not": {
                "terms": {
                    "company": [
                        "GE Aviation",
                        "GE Digital",
                        "GE Corporate",
                        "GE Power",
                        "GE Transportation",
                        "GE Renewable Energy",
                        "GE Oil & Gas",
                        "GE Gas Power",
                        "GE Energy Connections",
                        "GE Capital",
                        "GE Power Portfolio",
                        "FieldCore",
                        "Baker Hughes GE",
                        "Current & GE Lighting",
                        "GE Global Operations",
                        "ge appliances and lighting",
                        "GE Healthcare",
                        "GE Global Growth Organization"
                        ]
                    }
                }
            }
        }
    }
    return search_request(body)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--infile', nargs=1, help="JSON file to be processed", type=argparse.FileType('r'))
    arguments = parser.parse_args()
    config = json.load(arguments.infile[0])
    engine = get_connection(config["config_path"], config["connection_profile"])
    # setting up logger
    parent_path = os.path.dirname(os.path.abspath(__file__))
    log_filename = str(arguments.infile[0].name).split('/')[-1].replace('json', 'log')
    logger = setup_logger(os.path.join(parent_path, "logs", log_filename))
    urllib3.disable_warnings()
    dt = datetime.now(pytz.timezone('Asia/Kolkata'))
    logger.info("Ingestion Started")
    def get_auth_headers():
        request_body = config["request_body"]
        token = requests.post(
            config["auth_url"],
            data=request_body,
            headers={ "Accept": "application/json" },
            verify=False,
        ).json()
        headers = {
            "Content-Type": "application/json",
            "Authorization": "{} {}".format(token["token_type"], token["access_token"]),
            "x-api-key": config["x-api-key"]
        }
        return headers
    try:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        akana_headers = get_auth_headers()
        final_df=pd.DataFrame()
        for cmpny in config["companys_list"]:
            app_results = [a["_source"] for a in get_buss_app(cmpny)]
            size = len(app_results)
            num_batches = 16
            batch_size = int(size / num_batches)
            list = []
            threads = []
            for x in range(num_batches):
                if (x != num_batches-1):
                    threads.append(threading.Thread( target = extract_cis, args = (app_results[x*batch_size:(x+1)*batch_size],list,size,x) ))
                else:
                    threads.append(threading.Thread( target = extract_cis, args = (app_results[x*batch_size:(x+1)*batch_size+ size % num_batches],list,size,x) ))

            thread_index = 0
            for th in threads:
                th.start()
                thread_index = thread_index + 1

            thread_index2 = 0
            for th in threads:
                th.join()
                thread_index2 = thread_index2 + 1

            df = pd.DataFrame(list,columns = config["column"])
            final_df=pd.concat([final_df,df])
            fileName = 'output_business_app_{}_{}'.format(cmpny.replace(" & ","_").replace(" ","_"), datetime.now().strftime("%Y-%m-%d")+'.csv')
            df.to_csv(os.path.join(parent_path, "data", fileName), index = False)

        app_results = [a["_source"] for a in get_other_buss_app()]
        size = len(app_results)
        num_batches = 16
        batch_size = int(size / num_batches)
        list = []
        threads = []
        for x in range(num_batches):
            if (x != num_batches-1):
                threads.append(threading.Thread( target = extract_cis, args = (app_results[x*batch_size:(x+1)*batch_size],list,size,x) ))
            else:
                threads.append(threading.Thread( target = extract_cis, args = (app_results[x*batch_size:(x+1)*batch_size+ size % num_batches],list,size,x) ))

        thread_index = 0
        for th in threads:
            th.start()
            thread_index = thread_index + 1
        thread_index2 = 0
        for th in threads:
            th.join()
            thread_index2 = thread_index2 + 1

        df = pd.DataFrame(list, columns = config["column"])
        final_df=pd.concat([final_df,df])
        app_other_companies_fileName = 'output_business_app_other_companies_{}'.format(datetime.now().strftime("%Y-%m-%d")+'.csv')
        df.to_csv(os.path.join(parent_path, "data", app_other_companies_fileName),index = False)
        hosting_opal_business_data_fileName = 'hosting_opal_business_data_{}'.format(datetime.now().strftime("%Y-%m-%d")+'.csv')
        final_df.to_csv(os.path.join(parent_path, "data", hosting_opal_business_data_fileName),index=False)
        logger.info("Csv File generated")
        final_df.to_sql(config["table_name"], schema=config["schema_name"],if_exists='append',con=engine, index=False,method='multi',chunksize=1500)
        logger.info(f"Data Inserted to {config['schema_name']}.{config['table_name']}")
        logger.info("Ingestion Completed")
    except Exception as e:
        logger.error(f"some error occurred while executing {e}")
        send_email_notification(f"Exception occured {e} at {parent_path}", "Hosting Opal Ingestion Failure|Prod")
