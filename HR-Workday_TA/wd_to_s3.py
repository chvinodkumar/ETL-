import json
import asyncio
from datetime import datetime
from dotenv import load_dotenv
import os
import platform
import aiohttp
import logging
import xml.etree.ElementTree as ET
import logging
import logging.config
import aiofiles
import aiofiles.os
import ssl
import boto3
import pytz
from botocore.exceptions import NoCredentialsError
import argparse
import re

parser = argparse.ArgumentParser()
parser.add_argument('--infile', nargs=1, help="JSON file to be processed", type=argparse.FileType('r'))
arguments = parser.parse_args()

# Loading a JSON object returns a dict.
config = json.load(arguments.infile[0])

if platform.system()=='Windows':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def fetch_data() -> None:
    # config = read_config("config.json")
    set_logger(config)
    logger.info("Starting process of WD ingestion.")

    credentials = get_credentials(config["username"], config["password"])
    # proxy = config["proxies"]["https"]
    proxy = None
    datetime_est = datetime.now(tz=pytz.timezone('America/New_York'))
    sent_after = datetime_est.strftime("%Y-%m-%dT12:00:00.000-07:00")
    #sent_after = datetime.now().strftime("%Y-%m-%dT12:00:00.000-07:00")
    #sent_after = "2024-08-10T12:00:00.000-07:00"
    soap_data = get_soap_data(os.getenv(config["soap_data_path"]), credentials, sent_after)
    integrations = config['integrations']
    logger.info(
        f"Sent after date: {sent_after}. Number of integrations: {len(integrations)}."
    )

    responses = asyncio.run(
        send_integration_requests(
            config["integration_url"],
            soap_data,
            integrations,
            proxy=proxy,
        )
    )
    integrations_details, errors_post = extract_integration_details(responses)

    responses = asyncio.run(
        send_file_requests(
            config["download_url"],
            credentials,
            integrations_details,
            os.getenv(config["output_path"]),
            proxy=proxy,
        )
    )
    errors_get = [response for response in responses if response["status"] == "fail"]

    output_errors(errors_post, errors_get)
    logger.info("Process of WD ingestion has been finished.")


def read_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        config = json.loads(f.read())
    return config


def set_logger(config: dict) -> None:
    global logger
    log_config = config["logging"]
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)


def get_credentials(username_env_variable: str, password_env_variable: str) -> dict:
    load_dotenv()
    username = os.getenv(username_env_variable)
    password = os.getenv(password_env_variable)

    credentials = {"username": username, "password": password}

    return credentials


def get_soap_data(soap_data_path: str, credentials: dict, sent_after: str) -> str:
    with open(soap_data_path, "r") as f:
        soap_data = f.read()

    replace_dict = {
        "{username}": credentials["username"],
        "{password}": credentials["password"],
        "{sent_after}": sent_after,
    }

    for key in replace_dict:
        soap_data = soap_data.replace(key, replace_dict[key])

    return soap_data


async def send_integration_requests(
    url: str,
    soap_data: str,
    integrations: list[str],
    proxy: str = None,
    ssl_cert: ssl.SSLContext = None,
):
    logger.info(f"Fetching Document_IDs of integrations.")

    async with aiohttp.ClientSession() as session:
        requests = []
        for integration in integrations:
            integration_soap_data = soap_data.replace("{integration}", integration)
            requests.append(
                asyncio.create_task(
                    get_integration_details(
                        session,
                        url,
                        integration_soap_data,
                        integration,
                        proxy=proxy,
                        ssl_cert=ssl_cert,
                    )
                )
            )

        all_integrations_data = await asyncio.gather(*requests)

    return all_integrations_data


async def get_integration_details(
    session: aiohttp.ClientSession,
    url: str,
    soap_data: str,
    integration: str,
    proxy: str = None,
    ssl_cert: ssl.SSLContext = None,
) -> dict:
    try:
        async with session.post(url, data=soap_data, proxy=proxy, ssl=ssl_cert) as resp:
            response = await resp.content.read()
            content = response.decode()
            resp.raise_for_status()

    except aiohttp.ContentTypeError as e:
        response = generate_error_response(integration, resp.status, url, e)
    except aiohttp.ClientResponseError as e:
        response = generate_error_response(
            integration, resp.status, url, e, content=content
        )
    except asyncio.TimeoutError as e:
        response = generate_error_response(integration, 408, url, e)
    except aiohttp.ClientError as e:
        response = generate_error_response(integration, 500, url, e)
    else:
        response = {"status": "success", "integration/file": integration, "content": content}

    return response


def generate_error_response(
    integration: str, status_code: int, url: str, error: Exception, content: str = ""
) -> dict:
    response = {"status": "fail", "integration/file": integration}
    response["content"] = {
        "status_code": status_code,
        "url": url,
        "error_type": error.__class__.__name__,
        "error_message": content if content else error.args[0],
    }

    return response


def extract_integration_details(responses: list[dict]):
    integration_details = []
    integration_errors = []
    namespace = "{urn:com.workday/bsvc}"
    search_tag = "Integration_Repository_Document_Data"
    for response in responses:
        if response["status"] == "success":
            integration_found = False
            root = ET.fromstring(response["content"])
            document_data = root.findall(f".//{namespace}{search_tag}")
            for doc in document_data:
                if doc.attrib[f"{namespace}File_Name"].startswith("WD_INT"):
                    document_id = doc.attrib[f"{namespace}Document_ID"]
                    file_name = doc.attrib[f"{namespace}File_Name"]
                    file_name = file_name.replace(".pgp", "")
                    integration_details.append(
                        {"document_id": document_id, "file_name": file_name}
                    )
                    integration_found = True
                    break
            if not integration_found:
                response["status"] = "fail"
                response["content"] = {
                    "error_type": "Integration not found.",
                    "error_message": "Request was successful but response "
                    "did not contain any integration file."
                    }
                integration_errors.append(response)

        elif response["status"] == "fail":
            if response["content"]["error_type"] == "ClientResponseError":
                root = ET.fromstring(response["content"]["error_message"])
                fault_code = root.findall(".//faultcode")[0].text
                fault_string = root.findall(".//faultstring")[0].text
                response["content"]["error_message"] = f"{fault_code}: {fault_string}"

            integration_errors.append(response)

    return integration_details, integration_errors


async def send_file_requests(
    url: str,
    credentials: dict,
    integrations_details: list[dict],
    output_path: str,
    proxy: str = None,
    ssl_cert: ssl.SSLContext = None,
) -> list[dict]:
    logger.info(f"Downloading WD files.")

    aiohttp_auth = aiohttp.BasicAuth(credentials["username"], credentials["password"])

    async with aiohttp.ClientSession(auth=aiohttp_auth) as session:
        requests = []
        for integration_details in integrations_details:
            file_url = url + integration_details["document_id"]
            file_name = f"{output_path}{integration_details['file_name']}"
            requests.append(
                asyncio.create_task(
                    get_file(
                        session, file_url, file_name, proxy=proxy, ssl_cert=ssl_cert
                    )
                )
            )

        responses = await asyncio.gather(*requests)

    return responses


async def get_file(
    session: aiohttp.ClientSession,
    url: str,
    file_name: str,
    proxy: str = None,
    ssl_cert: ssl.SSLContext = None,
    chunk_size: int = 100e6,
) -> dict:
    response = None
    temp_file_name = file_name + "_tmp"

    try:
        async with session.get(url, proxy=proxy, ssl=ssl_cert) as resp:
            resp.raise_for_status()
            async with aiofiles.open(temp_file_name, "wb") as f:
                while True:
                    chunk = await resp.content.read(chunk_size)
                    if not chunk:
                        break
                    await f.write(chunk)

        await aiofiles.os.replace(temp_file_name, file_name)

    except aiohttp.ClientResponseError as e:
        error_dict = {
            529: "Invalid username.",
            401: "Invalid password.",
            404: "Document ID not found.",
        }
        error_message = error_dict.get(resp.status, "Unknown error.")
        response = generate_error_response(
            file_name, resp.status, url, e, content=error_message
        )
    except asyncio.TimeoutError as e:
        response = generate_error_response(file_name, 408, url, e)
    except aiohttp.ClientError as e:
        response = generate_error_response(file_name, 500, url, e)
    else:
        response = {"status": "success"}

    return response


def output_errors(errors_get: [list[dict]], errors_post: [list[dict]]) -> None:
    errors = errors_get + errors_post
    if errors:
        errors = "\n".join(list(map(lambda x: json.dumps(x), errors)))
        logger.error(
            f"Error(s) occured when ingesting following integration(s):\n{errors}"
        )
        send_email_notification("Innovation|HR-WORKDAY|Failed",f"Error(s) occured when ingesting following integration(s):\n{errors}")


def pick_files(bucket_name,profile_name,file_path):
    files=os.listdir(file_path)
    lis=list()
    for file in files:
        data=file.split('_')
        pattern='_'.join(data[:-2])
        lis.append(pattern)
    seq=list(set(lis))
    logger.info(f"total files to be loaded to s3 {len(files)}")
    boto3_logger = logging.getLogger('boto3')
    boto3_logger.setLevel(logging.WARNING)
    session = boto3.Session(profile_name=profile_name)
    s3 = session.client('s3')
    boto3_logger.setLevel(logging.NOTSET)
    for s in seq:
        for file in files:
            if s in file:
                year, month, day = datetime.now().year, datetime.now().month, datetime.now().day
                try:
                    s3.upload_file(file_path+file,bucket_name, f"Workday/{s}/year={year}/month={month}/day={day}/{file}")
                    os.remove(file_path+file)
                except NoCredentialsError:
                    logger.error("Credentials not available")
            else:
                pass
    send_email_notification("Innovation|Workday-Talent Acquisition|VALIDATION", f"{len(files)}Number of files loaded to s3")


def send_email_notification(subject, message, email_stake_holders=config['email_stake_holders']):
    try:
        os.system(f"""echo "{message}" | mailx -s "{subject}"  -a {config["logging"]["handlers"]["file"]["filename"]} {email_stake_holders}""")
    except Exception as err:
        logger.error(f'failed to send notification:\n{err}')


if __name__ == "__main__":
    try:
        fetch_data()
        HR_bucket = os.getenv(config['bucket_name'])
        HR_bucket_profile = os.getenv(config['profile_name'])
        files_path = os.getenv(config['output_path'])
        pick_files(bucket_name=HR_bucket, profile_name=HR_bucket_profile, file_path=files_path)
        logger.info(f"Files loaded to s3.")
    except Exception as err:
        logger.error(f'{err}')
        send_email_notification("Innovation|Workday-Talent Acquisition|Failed",f"Error(s) occured when ingesting following integration(s):\n{err}")
