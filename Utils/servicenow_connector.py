
#Importing required modules
import configparser
from oauthlib.oauth2 import LegacyApplicationClient
from requests_oauthlib import OAuth2Session
from requests.auth import HTTPBasicAuth

class ServiceNow_Authenticator:
    """
    A class ServiceNow_Authenticator which connects to ServiceNow using different autentications
    """
    def __init__(self, logger, creds_path, src_conn_profile):
        """
        The constructor for ServiceNow_Authenticator

        Parameters:
        logger (object)        : Logger object where log entries are to be made
        creds_path (Path) : Path of Servicenow Source credentials
        src_conn_profile  : Source connection profile name in creds_path
        """

        self.logger = logger
        self.creds_path = creds_path
        self.src_conn_profile = src_conn_profile

    def connect(self, authenticator_type):
        """
        A method to initiate type of authenticator method based on the inputs received

        Parameters:
        authenticator_type     : Type of the authenticator method
        Returns: Autentication object to the ServiceNow
        """
        if authenticator_type == 'oauth2':
            client = self.Oauth2_client(self.creds_path, self.src_conn_profile)
        if authenticator_type == 'basic_auth':
            client = self.BasicAuth_client(self.creds_path, self.src_conn_profile)
        return client

    def Oauth2_client(self, creds_path, src_conn_profile):
        """
        A method to Autenticate to ServiceNow using oauth2

        Parameters:
        creds_path (Path) : Path of Servicenow Source credentials
        src_conn_profile  : Source connection profile name in creds_path

        Returns:
        token_info  : Auth Token info Object to the ServiceNow
        """

        try :
            # Get authorization details using oauth2.0
            self.logger.info('Executing Oauth2_client module in class ServiceNow_Authenticator')
            def read_config_file(filepath, connection):
                config = configparser.ConfigParser()
                config.read(filepath)
                access_token_url = config[connection]['access_token_url']
                client_id = config[connection]['client_id']
                client_secret = config[connection]['client_secret']
                username = config[connection]['username']
                password = config[connection]['password']
                return access_token_url,client_id,client_secret,username,password

            access_token_url,client_id,client_secret,username,password = read_config_file(creds_path, src_conn_profile)
            oauth = OAuth2Session(client=LegacyApplicationClient(client_id=client_id))
            token_info = oauth.fetch_token(token_url=access_token_url,username=username,
                                            password=password,client_id=client_id,
                                            client_secret=client_secret)

            self.logger.info(f'Autentication to ServiceNow : Success')
            return token_info
        except Exception as e:
            self.logger.info(f'Autentication to ServiceNow : Failed : Error --> {e}')
            raise

    def BasicAuth_client(self, creds_path, src_conn_profile):
        """
        A method to Autenticate to ServiceNow using Basic Auth

        Parameters:
        creds_path (Path) : Path of Servicenow Source credentials
        src_conn_profile  : Source connection profile name in creds_path

        Returns:
        auth  : Authenticator Object to the ServiceNow
        """

        try :
            self.logger.info('Executing BasicAuth_client module in class ServiceNow_Authenticator')
            # Get authorization details using basic Auth
            def read_config_file(filepath, connection):
                config = configparser.ConfigParser()
                config.read(filepath)
                username = config[connection]['username']
                password = config[connection]['password']
                return username, password

            username,password = read_config_file(creds_path, src_conn_profile)
            auth = HTTPBasicAuth(username,password)
            self.logger.info(f'Autentication to ServiceNow : Success')
            return auth
        except Exception as e:
            self.logger.info(f'Autentication to ServiceNow : Failed : Error --> {e}')
            raise

