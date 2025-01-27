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

    with open(config["file_path"], "w", encoding="utf-8") as textfile:
        print(f'Programs - writing to file... {config["file_path"]}')
        textfile.write(json.dumps(json_res))
        logger.info(f"file stored path is {config["file_path"]}")

    with open(config["file_pathProgramId"], "w", encoding="utf-8") as textfileId:
        print(f'uq ProgramId list - writing to file... ({config["file_pathProgramId"]})')
        textfileId.write(str(uqProgramIdList).replace('{', '').replace('}', '') + "\n")
    return response
def response_convert_to_csv(config):
    df = pd.read_json(config["file_path"], orient='records')
    df.to_csv(config["file_path"].replace(".txt", ".csv"), index=False)

def main(config):
    logger.info("Main execution started")
    access_token = access_tok(config)
    get_program_data(config, access_token)
    response_convert_to_csv(config)




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






''''
import pandas as pd
import json
import requests
from datetime import date, datetime, timedelta
import argparse

curt_date = date.today()
pre_date = date.today() - timedelta(days=1)

def access_tok(config):
    url_cred = config["url_cred"]
    response = requests.get(url_cred, proxies=config.get("proxy", {}))
    access_token = response.json().get("access_token")
    print('access token: ' + access_token)
    return access_token

def get_program_data(config, access_token):
    maxReturn = config["maxReturn"]
    base_url = config["base_url"]
    file_path = config["file_path"]
    file_pathProgramId = config["file_pathProgramId"]

    offset = 0
    uqProgramIdList = set()
    json_res = []

    while True:
        stroff = str(offset)
        url = f"{base_url}?access_token={access_token}&maxReturn={maxReturn}&offset={stroff}&earliestUpdatedAt={pre_date}T00:00:00-05:00&latestUpdatedAt={curt_date}T00:00:00-05:00"
        
        response = requests.get(url, proxies=config.get("proxy", {}))

        # Stop if no data
        if "No assets found for the given search criteria." in response.text:
            print(f"No more data. End offset (out of limit): {stroff}")
            break

        json_res.extend(response.json().get("result"))

        # Collect all programIDs
        for obj in response.json().get("result"):
            uqProgramIdList.add(obj.get("id"))

        offset += maxReturn

        # Track progress
        if offset % 1000 == 0:
            print(f"Current offset: {stroff}")

    # Write programs data to a file
    with open(file_path, "w", encoding="utf-8") as textfile:
        print(f'Programs - writing to file... {file_path}')
        textfile.write(json.dumps(json_res))

    # Write program IDs to a separate file
    with open(file_pathProgramId, "w", encoding="utf-8") as textfileId:
        print(f'uq ProgramId list - writing to file... ({file_pathProgramId})')
        textfileId.write(str(uqProgramIdList).replace('{', '').replace('}', '') + "\n")

def convert_to_csv(config):
    df = pd.read_json(config["file_path"], orient='records')
    df.to_csv(config["file_path"].replace(".txt", ".csv"), index=False)

def main(config):
    access_token = access_tok(config)
    get_program_data(config, access_token)
    convert_to_csv(config)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download data and save to a file.")
    parser.add_argument('--infile', required=True, help="JSON file with configuration", type=argparse.FileType('r'))
    args = parser.parse_args()
    
    config = json.load(args.infile)
    
    try:
        main(config)
    except Exception as e:
        print(e)

'''