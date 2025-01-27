#### Importing Necessary Packages ####
from s3_connector import S3Connector
from io import BytesIO, StringIO
from datetime import datetime
import pandas as pd
import sys, os
import traceback

class S3Operations:
    def __init__(self, logger, profile_name, partition=None, aws_access_key_id=None, aws_secret_access_key=None, region_name=None, role_arn=None, session_name=None):
        """
        Initializes the S3Operations object and establishes an S3 connection.

        Parameters:
        logger (Logger)             : Logger object for logging.
        profile_name (str)          : AWS profile name.
        partition (str)             : Indicator for partitioning the file in S3 ('y' for yes, otherwise no).
        aws_access_key_id (str, optional)     : AWS access key ID for authentication (if not using profile).
        aws_secret_access_key (str, optional) : AWS secret access key for authentication (if not using profile).
        region_name (str, optional)           : AWS region name.
        role_arn (str, optional)              : ARN of the role to assume for access.
        session_name (str, optional)          : Session name for the assumed role.

        Returns:
        None
        """
        self.logger = logger
        self.partition=partition
        self.logger.info("S3Operations class has been initiated")
        self.profile = profile_name
        self.s3_client = S3Connector(logger, self.profile, aws_access_key_id, aws_secret_access_key, region_name, role_arn, session_name).s3_client

    def transform_and_load(self, data, data_path, bucket, prefix, file_name, datatypes=None):
        """
        Typecasts the data in the DataFrame to specified datatypes and uploads the parquet file to S3.

        Parameters:
        data (DataFrame)            : The pandas DataFrame to be transformed and uploaded.
        data_path (str)            : Local path where the Parquet file will be temporarily saved.
        bucket (str)               : S3 bucket name where the file will be uploaded.
        prefix (str)               : S3 prefix (path) for the file.
        file_name (str)            : Name of the file to be uploaded.
        datatypes (dict, optional) : Dictionary mapping column names to their expected data types for typecasting.

        Returns:
        None

        Notes:
        - Converts the pandas DataFrame to a Spark DataFrame, typecasts columns according to datatypes, writes the DataFrame to a Parquet file into a path provided as input and will upload the file to s3.
        """
        self.logger.info("Performing type casting using transform method in s3_operations class")
        java_path = os.popen('dirname $(dirname $(readlink -f $(which java)))').read().strip()
        os.environ['JAVA_HOME'] = java_path
        os.environ['PATH'] = f"{os.environ['JAVA_HOME']}/bin:{os.environ['PATH']}"
        from pyspark.sql import SparkSession
        import  pyspark.sql.functions as F
        from pyspark.sql.types import TimestampType, StringType, LongType, DateType, FloatType, IntegerType, BooleanType
        try:
            casting = {
                'int': IntegerType(),
                'boolean': BooleanType(),
                'timestamp': TimestampType(),
                'datetime': TimestampType(),
                'date': DateType(),
                'float': FloatType(),
                'long': LongType()
            }
            data = data.astype(str).replace('', None)
            spark = SparkSession.builder.appName("Pandas to Parquet").getOrCreate()
            spark_df = spark.createDataFrame(data)
            if datatypes:
                for col_name, dtype in datatypes.items():
                    spark_df=spark_df.withColumn(col_name,F.when(F.isnan(F.col(col_name)), F.lit(None)).otherwise(F.col(col_name)).cast(casting.get(dtype.lower(), StringType())))
            spark_df.coalesce(1).write.parquet(data_path, mode='overwrite')
            self.logger.info(f"Temporary Parquet file created at {data_path}")
            spark.stop()
            files = os.listdir(data_path)
            for file in files:
                if file.endswith(".snappy.parquet"):
                    full_file_path = os.path.join(data_path, file)
                    try:
                        self.s3_client.upload_file(full_file_path, bucket, prefix + file_name)
                        self.logger.info(f"{full_file_path} uploaded to {bucket}{prefix} successfully ")
                        os.remove(full_file_path)
                        self.logger.info(f"{full_file_path} has been removed")
                    except Exception as e:
                        self.logger.error(f"Failed to Upload {full_file_path} to {bucket}{prefix}, with error --> {e} {traceback.format_exc()}")
                        raise
        except Exception as e:
            self.logger.error(f"Failed to type cast data in transform method in s3_operations module with error --> {e} {traceback.format_exc()}")
            raise

    def upload_file(self, file_name, file_type, data, bucket,prefix,data_path=None, dtypes=None,hour_partition=None):
        """
        Uploads a file to an S3 bucket.

        Parameters:
        file_name (str)  : The file name.
        file_type (str)  : Type of file to be uploaded ('csv', 'excel', 'parquet').
        data (DataFrame) : The data to upload.
        data_path (str)  : Local path where the temporary file (for Parquet) will be saved.
        bucket (str)     : The bucket to upload to.
        prefix (str)     : The S3 prefix (path) for the file.
        dtypes (dict, optional) : Dictionary mapping column names to their expected data types for typecasting (relevant for Parquet files).

        Returns:
        None

        Notes:
        - Handles the uploading of various file types (CSV, Excel, Parquet) to S3.
        """
        try:
            self.logger.info("Performing upload_file method to upload file to s3 in s3_operations class")
            if self.partition.lower() == 'y':
                object_name = prefix + f"year={datetime.today().strftime('%Y')}/month={datetime.today().strftime('%m')}/day={datetime.today().strftime('%d')}/"
                if hour_partition:
                    if hour_partition.lower()=='y':object_name = f"{object_name}hour={datetime.today().strftime('%H')}/"
            else:
                object_name = prefix
            if file_type == 'parquet':
                self.transform_and_load(data=data, datatypes=dtypes, file_name=file_name, bucket=bucket, prefix=object_name, data_path=data_path)
                self.logger.info(f"{file_name} has been uploaded to {object_name}")
            else:
                buffer_data = StringIO()
                if file_type.lower() == 'csv':
                    buffer_data = StringIO()
                    data.to_csv(buffer_data, index=False)
                elif file_type.lower() == 'excel':
                    buffer_data = BytesIO()
                    data.to_excel(buffer_data, index=False)
                else:
                    raise ValueError("Unsupported file_type provided.")
                buffer_data.seek(0)
                byte_data = BytesIO(buffer_data.getvalue().encode('utf-8'))
                self.s3_client.upload_fileobj(byte_data, bucket, object_name + file_name)
                self.logger.info(f"File {file_name} uploaded successfully to {bucket}/{object_name}.")
        except Exception as e:
            self.logger.error(f"Failed to upload file {file_name} to {bucket}/{object_name}, error --> {e} {traceback.format_exc()}")
            raise

    def getobject_s3(self, key, file_type,bucket_name, extra_features=None):
        """
        Retrieves an object from S3 and returns it as a pandas DataFrame.

        Parameters:
        key (str)                       : The S3 object key (file path).
        file_type (str)                 : Type of file ('csv', 'excel', 'parquet').
        extra_features (dict, optional) : Additional parameters for pandas read function.

        Returns:
        DataFrame: The data loaded into a pandas DataFrame if file is found.

        Notes:
        - Retrieves a file from S3 and returns it as a pandas DataFrame, with support for various file types.
        """
        try:
            self.logger.info("Performing getobject_s3 method to fetch files from s3 in s3_operations class")
            response = self.s3_client.get_object(Bucket=bucket_name, Key=key)
            data = response['Body'].read().decode('utf-8') if file_type.lower() == "csv" else response['Body'].read()
            read_functions = {
                'csv': pd.read_csv,
                'excel': pd.read_excel,
                'parquet': pd.read_parquet
            }
            buffer = StringIO(data) if file_type.lower() in ['csv', 'excel'] else BytesIO(data)
            df = read_functions[file_type.lower()](buffer, **(extra_features or {}))
            return df
        except Exception as e:
            self.logger.error(f"Failed executing getobject_s3 method in s3_operations class to retrieve object {key} from S3 with error --> {e} {traceback.format_exc()}")
            raise

    def list_files(self, bucket, file_name, prefix, marker=None):
        """
        Lists files in a specific S3 bucket with an optional prefix.

        Parameters:
        bucket (str)           : The bucket name.
        file_name (str)        : The name or pattern of the files to list.
        prefix (str, optional) : The prefix of the files to list.
        marker (str, optional) : A marker to continue listing files from a specific point.

        Returns:
        keys (list)            : A list of matching filenames in the specified S3 bucket if found.
        """
        try:
            self.logger.info("Performing list_files method to list files from s3 prefix in s3_operations class")
            keys = []
            if marker:
                response = self.s3_client.list_objects(Bucket=bucket, Delimiter="/", Prefix=prefix, Marker=marker)
            else:
                response = self.s3_client.list_objects(Bucket=bucket, Delimiter="/", Prefix=prefix)
            self.logger.info("Connected to S3")
            if response["ResponseMetadata"].get("HTTPStatusCode", None) == 200:
                for data in response.get('Contents', []):
                    if file_name in data["Key"]:
                        keys.append(data["Key"])
                if response.get('IsTruncated')==True:
                    self.logger.info("Response has been truncated, going to next page using list_files method in s3_operations class")
                    keys.extend(self.list_files(bucket=bucket, file_name=file_name, prefix=prefix, marker=response.get('NextMarker')))
            else:
                self.logger.info(f'failed with - {response["ResponseMetadata"].get("HTTPStatusCode", "unable to fetch status code")}')
                raise Exception
            if not keys:
                self.logger.info("No files found")
                sys.exit(1)
            return keys
        except Exception as e:
            self.logger.error(f"Failed in executing list_files method in s3_operations class to list files in bucket {bucket} with error --> {e}")
            raise

    def s3_to_s3_copy(self,source_bucket, source_key, dest_bucket, dest_key):
        """
        Copies an object from one S3 location to another S3 location.

        Parameters:
        source_bucket (str): The source bucket name.
        source_key (str): The source object key (path in S3).
        dest_bucket (str): The destination bucket name.
        dest_key (str): The destination object key (path in S3).

        Returns:
        response (dict): The response from the copy operation.
        """
        try:
            if self.partition.lower()=='y':
                object=dest_key+f"year={datetime.today().strftime('%Y')}/month={datetime.today().strftime('%m')}/day={datetime.today().strftime('%d')}/"
            else:object=dest_key
            copy_source = {'Bucket': source_bucket, 'Key': source_key}
            response = self.s3_client.copy_object(CopySource=copy_source, Bucket=dest_bucket, Key=object)
            self.logger.info(response)
            self.logger.info(f"{source_bucket}{source_key} has been copied to {dest_bucket}{dest_key}")
        except Exception as e:
            self.logger.error(f"Failed executing  method in s3_operations class with error --> {e} {traceback.format_exc()}")
            raise

    def s3_to_s3_move(self,source_bucket, source_key, dest_bucket, dest_key):
        """
        Copies an object from one S3 location to another S3 location and deletes the file from source s3 bucket.

        Parameters:
        source_bucket (str): The source bucket name.
        source_key (str): The source object key (path in S3).
        dest_bucket (str): The destination bucket name.
        dest_key (str): The destination object key (path in S3).

        Returns:
        response (dict): The response from the copy operation.
        """
        try:
            self.s3_to_s3_copy(source_bucket=source_bucket,source_key=source_key,dest_bucket=dest_bucket,dest_key=dest_key)
            response=self.s3_client.delete_object(Bucket=source_bucket, Key=source_key)
            self.logger.info(response)
            self.logger.info(f"{source_bucket}{source_key} has been deleted")
        except Exception as e:
            self.logger.error(f"Failed executing  s3_to_s3_move method in s3_operations class with error --> {e} {traceback.format_exc()}")
            raise
