import os
import sys
import argparse
import json
import logging
from datetime import datetime
import requests
import traceback


def setup_logger(log_file):
    logging.basicConfig(
        filename=log_file,
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(message)s",
        filemode="a"
    )
    logger = logging.getLogger()
    return logger


def send_email_notification(subject, message):
    print(f"Email sent: {subject} - {message}")


def download_data(url, headers, file_path):
    try:
        logger.info(f"Downloading data from {url}")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        with open(file_path, "wb") as f:
            f.write(response.content)
        logger.info(f"File saved successfully to {file_path}")

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download the data: {str(e)}")
        send_email_notification("Download Failed", f"Error: {str(e)}")
        traceback.print_exc()


def main(config_file):
    try:
        with open(config_file, 'r') as infile:
            config = json.load(infile)
        url = config["url"]
        headers = config["headers"]
        file_path = os.path.join(config["filepath"], "welcomecdf.csv")
        download_data(url, headers, file_path)

    except Exception as e:
        logger.error(f"An error occurred in main: {str(e)}")
        send_email_notification("Script Failed", f"Error in main: {str(e)}")
        message = config["email"]["recipient"]
        subject = config["email"]["subject"]
        traceback.print_exc()


if __name__ == "__main__":
    start_time = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    parser = argparse.ArgumentParser(description="Download data and save to a file.")
    parser.add_argument('--infile', required=True, help="JSON file with configuration", type=str)
    args = parser.parse_args()
    config_file = args.infile
    log_filename = os.path.basename(config_file).replace('.json', '.log')
    log_file_path = os.path.join(os.getcwd(), log_filename)
    logger = setup_logger(log_file_path)
    logger.info("Starting the script...")
    main(config_file)
    logger.info(f"Script finished at {datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}")
