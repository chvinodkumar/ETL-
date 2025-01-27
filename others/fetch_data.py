import asyncio
import csv
import json
import logging
import logging.config
import os
import platform
from datetime import datetime

import aiohttp
import redshift_connector
import requests
import urllib3
from dotenv import load_dotenv

import io
import boto3
from botocore.exceptions import NoCredentialsError
if platform.system()=='Windows':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
urllib3.disable_warnings()
load_dotenv()
env = "prod"

import argparse
import json
parser = argparse.ArgumentParser()
parser.add_argument('--infile', nargs=1, required=True, help="JSON file to be processed", type=argparse.FileType('r'))
arguments = parser.parse_args()
config = json.load(arguments.infile[0])

def fetch_data():
    set_logger(config)
    logger.info("Starting process of OpAL ingestion.")

    try:
        cursor = get_redshift_cursor(**config[env]["redshift"]["credentials"])
        id_list = get_sso_id_list(cursor, config[env]["redshift"]["schema"])

        headers = get_headers(**config[env]["opal"]["headers_data"])
        responses = asyncio.run(
            get_id_flags(id_list, config[env]["opal"]["url"], headers)
        )

        flagged_id_list = get_flagged_id_list(id_list, responses)
        write_to_csv(flagged_id_list, config["output_headers"])

    except RequestError as e:
        logger.error(f"One of the query requests failed. Error details: {e}.")
    except Exception:
        logger.exception("An unexpected error occurred.")
    else:
        logger.info("The process of OpAL ingestion has been finished.")


class RequestError(Exception):
    pass


def read_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        config = json.loads(f.read())
    return config


def set_logger(config: dict) -> None:
    global logger
    log_config = config["logging"]
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)


def get_redshift_cursor(
    host: str, port: int, database: str, user: str, password: str
) -> redshift_connector.Cursor:
    logger.info(f"Connecting to Redshift database with host: {host}.")
    connection = redshift_connector.connect(
        host=host,
        port=os.getenv(port),
        database=os.getenv(database),
        user=os.getenv(user),
        password=os.getenv(password),
    )

    cursor = connection.cursor()
    return cursor


def get_sso_id_list(cursor: redshift_connector.Cursor, schema: str) -> list[str]:
    logger.info("Executing query on database.")
    cursor.execute(
        f"""
        SELECT
            DISTINCT EMP_SSO_ID
        FROM
            {schema}.EMP_PROFILE_ALL
        WHERE
            SOURCE IN ('WORKDAY', 'CDI')
            AND CURR_FLG = 'Y'
        """
    )

    result = cursor.fetchall()
    id_list = [row[0] for row in result]

    return id_list


def get_headers(
    client_id: str, client_secret: str, access_token_url: str, api_key: str
) -> dict[str, str]:
    access_token = generate_access_token(client_id, client_secret, access_token_url)

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"{access_token['token_type']} {access_token['access_token']}",
        "X-API-KEY": os.getenv(api_key),
    }

    return headers


def generate_access_token(
    client_id: str, client_secret: str, access_token_url: str
) -> dict[str, str]:
    logger.info("Generating OpAL access token.")
    request_body = {
        "client_id": os.getenv(client_id),
        "client_secret": os.getenv(client_secret),
        "scope": "api",
        "grant_type": "client_credentials",
    }

    token = requests.post(
        access_token_url,
        data=request_body,
        headers={"Accept": "application/json"},
        verify=False,
    ).json()

    return token


async def get_id_flags(
    id_list: list[str], url: str, headers: dict[str, str], timeout: int = 30
):
    id_batches = split_list_to_batches(id_list, 256)
    request_batches = split_list_to_batches(id_batches, 50)

    timeout = aiohttp.ClientTimeout(connect=timeout)
    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
        logger.info(f"Sending {len(id_batches)} query requests.")

        responses = []
        for request_batch in request_batches:
            requests = []
            for id_batch in request_batch:
                body = get_request_body(id_batch)
                requests.append(asyncio.create_task(send_request(session, url, body)))

            responses += await asyncio.gather(*requests)

        return responses


def split_list_to_batches(input_list: list[str], batch_size: int) -> list[list]:
    batches = [
        input_list[i * batch_size : (i + 1) * batch_size]
        for i in range((len(input_list) + batch_size - 1) // batch_size)
    ]

    return batches


def get_request_body(sso_list: list) -> dict:
    query_template = "(assigned_to_sso:{sso} AND sys_class_name:cmdb_ci_pc_hardware)"
    queries = [query_template.format(sso=sso) for sso in sso_list]
    query = ", ".join(queries)
    body = {
        "size": 1024,
        "_source": ["assigned_to_sso"],
        "query": {"query_string": {"query": query}},
    }

    body = json.dumps(body)

    return body


# DECORATOR
# In case request fails and response contains specified status code
# a number of retries is made to send request again
def retry_on_error(request_function, request_retries: int = 3):
    async def wrapper(*args, **kwargs):
        for retry_count in range(0, request_retries + 1):
            response = await request_function(*args, **kwargs)
            if (
                "status_code" in response
                and (response["status_code"] >= 500 or response["status_code"] == 408)
                and retry_count < request_retries
            ):
                await asyncio.sleep(3)
            else:
                break
        if "status_code" in response:
            send_email_notification("PROD|HR-OPAL|Failed",f"Error(s) occured when ingestion :\n{response}")
            #logger.error(f"resp: {response}")
            raise RequestError(response)
        return response

    return wrapper


@retry_on_error
async def send_request(session: aiohttp.ClientSession, url: str, body: str):
    try:
        async with session.post(url, data=body ) as resp:
            response = await resp.json()
            resp.raise_for_status()

    except aiohttp.ContentTypeError as e:
        response = generate_error_response(resp.status, e)
    except aiohttp.ClientResponseError as e:
        response = generate_error_response(
            resp.status,
            response.get("message", e.__class__.__name__),
            response.get("error", e),
        )
    except asyncio.TimeoutError as e:
        response = generate_error_response(408, e)
    except aiohttp.ClientError as e:
        response = generate_error_response(500, e)

    return response


def generate_error_response(
    status_code: int, error_message: str, error: str = None
) -> dict:
    response = {
        "status_code": status_code,
        "error_message": error_message,
        "error": error,
    }

    return response


def get_flagged_id_list(all_id_list: list[str], responses: list[dict]) -> list[str]:
    logger.info("Extracting output data from responses.")

    found_ids = set()
    for response in responses:
        for hit in response["hits"]["hits"]:
            found_ids.add(hit["_source"]["assigned_to_sso"])

    missing_ids = set(all_id_list) - found_ids
    y_flagged_ids = [[id, "Y"] for id in found_ids]
    n_flagged_ids = [[id, "N"] for id in missing_ids]
    flagged_id_list = y_flagged_ids + n_flagged_ids

    return flagged_id_list


def write_to_csv(
    rows: list[list[str]], columns: list[str], delimiter: str=chr(127)
) -> None:
    timestamp = datetime.now().strftime("%m%d%Y_%H%M%S")
    file_name = f"output/OpAL_data_full_{timestamp}.csv"
    logger.info(f"Writing output to file: {file_name}.")
    session= boto3.Session(profile_name='prod')
    bucket_name='odp-us-prod-hr-upstream'
    year, month, day = datetime.now().year, datetime.now().month, datetime.now().day
    s3_key=f"OPAL/year={year}/month={month}/day={day}/OpAL_data_full_{timestamp}.csv"
    csv_data = io.StringIO()
    writer = csv.writer(csv_data, delimiter=delimiter)
    writer.writerow(columns)
    writer.writerows(rows)
    s3 = session.client('s3')
    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_data.getvalue())
    logger.info(f"CSV file has been uploaded to {bucket_name} with the key {s3_key}")
    send_email_notification("PROD|HR-OPAL|VALIDATION", f"OpAL_data_full_{timestamp}.csv file loaded to s3")

def send_email_notification(subject, message, email_stake_holders=config['email_stake_holders']):
    try:
        os.system(f"""echo "{message}" | mailx -s "{subject}"  -a {config["logging"]["handlers"]["file"]["filename"]} {email_stake_holders}""")
    except Exception as err:
        logger.info(f'failed to send notification')


if __name__ == "__main__":
    fetch_data()
