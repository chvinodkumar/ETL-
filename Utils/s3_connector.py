
#### Importing Necessary Package ####
import boto3

class S3Connector:
    def __init__(self, logger, profile_name=None, aws_access_key_id=None, aws_secret_access_key=None, region_name=None, role_arn=None, session_name=None):
        """
        Initializes the S3Connector object.

        Parameters:
        logger (Logger)             : Logger object for logging.
        profile_name (str)          : AWS profile name.
        aws_access_key_id (str)     : AWS access key ID.
        aws_secret_access_key (str) : AWS secret access key.
        region_name (str)           : AWS region name.
        role_arn (str)              : ARN of the role to assume.
        session_name (str)          : Session name for the assumed role.

        Returns: None
        """
        self.logger = logger
        self.logger.info("Executing S3Connector class to enable S3 connection")
        self.aws_access_key = aws_access_key_id
        self.aws_secret_key = aws_secret_access_key
        self.region_name = region_name
        self.role_arn = role_arn
        self.session_name = session_name

        # Initialize the session
        try:
            self.logger.info("Initializing S3 session")
            if self.aws_access_key and self.aws_secret_key:
                self.s3_session = boto3.Session(aws_access_key_id=self.aws_access_key_id,aws_secret_access_key=self.aws_secret_access_key,region_name=self.region_name)
            else:self.s3_session = boto3.Session(profile_name=profile_name)
            self.logger.info("Boto3 session initialized successfully.")
        except Exception as e:
            self.logger.error(f"Failed to initialize boto3 session, error --> {e}")
            raise

        # Assume role if role ARN and session name are provided
        self.credentials = None
        if self.role_arn and self.session_name:
            try:
                self.logger.info("ARN Role and Session Name found, temporary role will be established")
                self.credentials = self.assume_role()
            except Exception as e:
                self.logger.error(f"Role assumption failed with error --> {e}")
                raise

        # Establish S3 connection
        try:
            self.s3_client = self.s3_connection()
            self.logger.info("S3 connection established successfully.")
        except Exception as e:
            self.logger.error(f"Failed to establish S3 connection with error --> {e}")
            raise

    def s3_connection(self):
        """Establishes the S3 connection

        Parameter:None

        Returns:
        s3_session_client (object) : The S3 client object.
        """
        self.logger.info("Executing s3_connection method")
        try:
            if self.credentials:
                self.logger.info("Connecting using assumed role credentials")
                s3_client= self.s3_session.client('s3',
                    aws_access_key_id=self.credentials["access_key"],
                    aws_secret_access_key=self.credentials['secret_key'],
                    aws_session_token=self.credentials['session_token'],
                    region_name=self.region_name
                )
                self.logger.info("Connection using assumed role successful")
            else:
                s3_client=self.s3_session.client('s3')
                self.logger.info("S3 client connection successful")
            return s3_client
        except Exception as e:
            self.logger.error(f"Failed to establish s3 connection with error -->{e}")
            raise

    def assume_role(self):
        """Assumes an AWS IAM role and returns temporary credentials

        Parameter:None

        Returns:
        assumed_credentials (dict) : A dictionary containing the temporary credentials.
        """
        self.logger.info("Executing assume_role method")
        try:
            sts_client = boto3.client('sts')
            self.logger.info("S3 client established for assume_role")
            response = sts_client.assume_role(
                RoleArn=self.role_arn,
                RoleSessionName=self.session_name
            )
            credentials = response['Credentials']
            assumed_credentials={
                'access_key': credentials['AccessKeyId'],
                'secret_key': credentials['SecretAccessKey'],
                'session_token': credentials['SessionToken']
            }
            self.logger.info("Role assumed successfully.")
            return assumed_credentials
        except Exception as e:
            self.logger.error(f"Failed to execute assume_role method with error --> {e}")
            raise
