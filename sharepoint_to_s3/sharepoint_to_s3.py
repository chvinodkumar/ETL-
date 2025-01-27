"""
{
    "log_file_path": "folder path with Log file name",
    "mail_notifications" :
    {
      "subject" : "Mail_subject",
      "Message_execution" : "Exception while copying files from SharePoint to S3",
      "dl" : "mail_dl"
    },
    "tenant_id": "tenant_id",
    "client_id": "client_id",
    "client_secret": "client_secret",
    "resource_url": "https://graph.microsoft.com/",
    "site_url": "sharepoint_site_url",
    "folder_mask" :
    {
          "drive_folder" : "drive_folder_name",
          "parent_folder" : "parent_folder_name",
          "child_folders_list" : ["list of child folder names"]
    },
    "delete_file_from_sharepoint" : true/flase (true - delete , false - not)",
    "Target" :
    {
        "profilename" : "profilename_s3_bucket",
        "bucketname" : "s3_bucket_name"
    }
}

"""
#Starting the script#

#Import the required modules
import requests
import os,csv,logging
import sys,json
import boto3

#Class to copy files from sharepoint to s3 and delete the file in sharepoint
import requests
import os,csv,logging
import sys,json
import boto3

#Class to copy files from sharepoint to s3 and delete the file in sharepoint
class SharePointClient:
    def __init__(self, config):
        self.config = config
        self.logger = self.custom_logger()
        self.base_url = f"https://login.microsoftonline.com/{self.config['tenant_id']}/oauth2/v2.0/token"
        self.main_url = f"{self.config['resource_url']}/v1.0/sites/"
        self.access_token = self.get_access_token()  # Initialize and store the access token upon instantiation
        self.headers = {'Authorization': f'Bearer {self.access_token}'}
        self.s3_client = self.s3_connect()
        self.site_id = self.get_site_id()
        self.drive_id = self.get_drive_id()
        self.folder_ids = self.get_folder_id()
        self.copy_folders_to_s3(self.folder_ids)

    def custom_logger(self):
        with open(f'{self.config["log_file_path"]}', 'a', newline='') as csvfl:
            write_csv = csv.writer(csvfl, delimiter='|')
            write_csv.writerow(['\t TimeStamp \t', 'Severity', '\t Message \t'])
        logging.basicConfig(filename=self.config["log_file_path"],level=logging.INFO, format=('%(asctime)s | %(levelname)s | %(message)s'))
        logger = logging.getLogger('hvr')
        return logger

    def send_mail_notifications(self, error):
        message_failure = f'{self.config["mail_notifications"]["Message_execution"]}.Error : {str(error)[:1000]}'
        os.system(f' echo "{message_failure}. Refer to logfile : {self.config["log_file_path"]}" | mailx -s "{self.config["mail_notifications"]["subject"]}"  {self.config["mail_notifications"]["dl"]}')

    def s3_connect(self):
        try:
            session = boto3.Session(profile_name=self.config["Target"]["profilename"])
            s3_client = session.client('s3')
            return s3_client    # Return S3 client
        except Exception as e:
            logging.error(f'Error while establishing the s3 clinet : {e}')
            self.send_mail_notifications(e)

    def fetch_data(self,url):
        # get response through api call
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response   # return response
        except Exception as e:
            self.logger.info(f"Exception while fetching the data : {e}")
            self.send_mail_notifications(e)

    def get_access_token(self):
        # Body for the access token request
        body = {
            'grant_type': 'client_credentials',
            'client_id': self.config["client_id"],
            'client_secret': self.config["client_secret"],
            'scope': self.config["resource_url"] + '.default'
        }
        response = requests.post(self.base_url, headers={'Content-Type': 'application/x-www-form-urlencoded'}, data=body)
        return response.json().get('access_token')  # Extract access token from the response

    def get_site_id(self):
        # Build URL to request site ID
        full_url = f'{self.main_url}{self.config["site_url"]}'
        response = self.fetch_data(full_url).json()
        return response.get('id')  # Return the site ID

    def get_drive_id(self):
        # Retrieve drive IDs and names associated with a site
        drives_url = f'{self.main_url}{self.site_id}/drives'
        drives = self.fetch_data(drives_url).json()
        if 'value' in drives:
            return next((drive['id'] for drive in drives.get('value', []) if drive['name'] == self.config["folder_mask"]["drive_folder"]), None)  # Retrun the drive ID

    def get_folder_id(self):
        # Retrieve ID of sub folders
        folder_url = f'{self.main_url}{self.site_id}/drives/{self.drive_id}/root/children'
        response = self.fetch_data(folder_url) # fetch response from urll
        folder_data = response.json()

        parent_folder_id = None

        if "value" in  folder_data:
            parent_folder_id = next(map(lambda item: (item["id"],item['name']), filter(lambda item: item["name"] == self.config["folder_mask"]["parent_folder"], folder_data.get("value",[])))) #get parent folder ID

        sub_folders = []
        if len(self.config["folder_mask"]["child_folders_list"]) > 0 :  #IF contents in child_folders_list
            folder_path = f'items/{parent_folder_id[0]}'
            sub_folder_url = f'{self.main_url}{self.site_id}/drives/{self.drive_id}/{folder_path}/children'
            response = self.fetch_data(sub_folder_url)
            sub_folder_data = response.json()

            if "value" in folder_data:
                sub_folders = list(map(lambda item: (item["id"],item['name']), filter(lambda item: item["name"] in self.config["folder_mask"]["child_folders_list"],sub_folder_data.get("value",[]))))

        return sub_folders if len(sub_folders) > 0 else parent_folder_id

    def delete_item(self, delete_url, file_name):
        # delete file in Site
        try :
            response = requests.delete(delete_url, headers= self.headers)
            response.raise_for_status()
            self.logger.info(f'File deleted from sharepoint : {file_name}')
        except Exception as e:
            self.logger.info(f"Exception While deleting the files : {e}")
            self.send_mail_notifications(e)

    def to_s3(self, download_url, file_name):
        # Copy response to S3
        response = self.fetch_data(download_url)
        self.s3_client.put_object(Body=response.content, Bucket=self.config["Target"]["bucketname"], Key=file_name)
        self.logger.info(f"File copied to S3: {file_name}")
        return 'success'

    def copy_folders_to_s3(self, folder_ids, folder_path= '', level=0):
        # Recursively get all contents from a folder
        if not isinstance(folder_ids, list):
            folder_ids = [folder_ids]  # Ensure folder_ids is a list

        for folder_id,folder_name in folder_ids:
            try:
                folder_contents_url = f'{self.main_url}{self.site_id}/drives/{self.drive_id}/items/{folder_id}/children'
                folder_contents = self.fetch_data(folder_contents_url).json()

                if 'value' in folder_contents :
                    for item in folder_contents['value']:
                        if 'folder' in item:
                            new_path = os.path.join(folder_path, item['name'] )
                            self.copy_folders_to_s3((item['id'], folder_name), new_path, level + 1)  # Recursive call for subfolders
                        elif 'file' in item:
                            file_url = f'{self.main_url}{self.site_id}/drives/{self.drive_id}/items/{item["id"]}'
                            file_name = os.path.join(folder_name, folder_path, item['name'])
                            file_download_url = f'{file_url}/content'
                            copy_file = self.to_s3(file_download_url, file_name)   #copy file to s3
                            if copy_file == 'success' and self.config['delete_file_from_sharepoint']== True:
                                self.delete_item(file_url, file_name)      #Delete file from site
            except Exception as e:
                self.logger.info(f"Exception while copying the files : {e}")
                self.send_mail_notifications(e)

if __name__ == '__main__':
    try :
        ## passing the json path
        ARGV = sys.argv
        json_path = ARGV[1]

        with open(json_path) as p:
            contents = json.load(p)
            p.close()

        SharePointClient(config = contents)  #calling the class to copy files from sharepoint to s3
    except Exception as e:
        os.system(f'echo "{contents["mail_notifications"]["Message_execution"]}. Error : {e} . {contents["log_file_path"]}" | mailx -s "{contents["mail_notifications"]["subject"]}"  {contents["mail_notifications"]["dl"]}')

