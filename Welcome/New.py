import os
import sys
import argparse
import json
import logging
from datetime import datetime
import requests
import traceback

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
        message=config["email"]["recipient"]
        subject=config["email"]["subject"]
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


'''import os
import sys
import argparse
import json
import logging
from datetime import datetime
import requests
import boto3
from io import BytesIO
import traceback

# Function to setup logger
def setup_logger(log_file):
    logging.basicConfig(
        filename=log_file,
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(message)s",
        filemode="a"
    )
    logger = logging.getLogger()
    return logger

# Function to send email notifications (mockup)
def send_email_notification(subject, message):
    # Replace with actual email sending logic
    print(f"Email sent: {subject} - {message}")

# Function to download data from the provided URL and save to file
def download_data(url, headers, file_path):
    try:
        logger.info(f"Downloading data from {url}")
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx, 5xx)

        # Write the content to the file
        with open(file_path, "wb") as f:
            f.write(response.content)
        logger.info(f"File saved successfully to {file_path}")

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download the data: {str(e)}")
        send_email_notification("Download Failed", f"Error: {str(e)}")
        traceback.print_exc()

# Function to upload files from Box to S3
def box_s3(client, folder_id):
    """
    A method to download files from Box and upload files into S3.
 
    Parameters:
    client (object) : Client object which will be used to access the Box folder
    folder_id (str) : Box folder id
 
    Returns: None
    """
    for item in client.folder(folder_id).get_items():
        if item.type == "file" and item.name == config["file_name"]:
            buffer = BytesIO()
            client.file(item.id).download_to(buffer)
            buffer.seek(0)

            # Define the S3 object key based on partitioning setting
            if config["s3_partition"] == "Y":
                obj = config["s3_prefix"] + f"/year={year}/month={month}/day={day}/" + config["file_name"]
            else:
                obj = config["s3_prefix"] + "/" + config["file_name"]

            # Upload to S3
            session = boto3.Session(profile_name=config["s3_profile"])
            s3 = session.client('s3')
            s3.upload_fileobj(buffer, config["bucket_name"], obj)
            logger.info(f"{config['file_name']} uploaded to S3 successfully")
            break  # Exit loop after finding the file
    else:
        logger.error(f"File '{config['file_name']}' not found in Box folder with ID '{folder_id}'")

# Main function to handle the logic
def main(config_file):
    try:
        # Load configuration from JSON
        with open(config_file, 'r') as infile:
            global config  # Make config globally accessible
            config = json.load(infile)

        url = config["url"]
        headers = config["headers"]
        file_path = os.path.join(config["filepath"], "welcomecdf.xlsx")
        
        # Download the data
        download_data(url, headers, file_path)

        # Here, you would need to create or get your Box client.
        # Assuming a placeholder function `create_box_client()` that returns the client
        box_client = create_box_client()  # Define this function based on your Box SDK usage
        box_s3(box_client, config["folder_id"])

    except Exception as e:
        logger.error(f"An error occurred in main: {str(e)}")
        send_email_notification("Script Failed", f"Error in main: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    # Start time
    start_time = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")

    # Set up argument parsing
    parser = argparse.ArgumentParser(description="Download data and save to a file.")
    parser.add_argument('--infile', required=True, help="JSON file with configuration", type=str)
    args = parser.parse_args()

    # Set up logging
    config_file = args.infile
    log_filename = os.path.basename(config_file).replace('.json', '.log')
    log_file_path = os.path.join(os.getcwd(), log_filename)
    logger = setup_logger(log_file_path)

    # Run the main function
    logger.info("Starting the script...")
    main(config_file)
    logger.info(f"Script finished at {datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}")
'''