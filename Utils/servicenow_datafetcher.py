
#Importing required modules
import requests
import os
import time
from datetime import datetime
import pandas as pd
from servicenow_connector import ServiceNow_Authenticator
from redshift_connector import get_connection

class ServiceNow_DataFetcher:
    def __init__(self, logger) -> None:
        """
        A class to fetch data from the ServiceNow API.

        This class provides methods to interact with the ServiceNow API by fetching data.
        It supports retrying requests in case of failures and includes basic logging to track operations and errors.

        Attributes:
            logger (object): A logger Object to log informational messages and errors.

        Methods:
            get_count()  : Fetches the total count of records from the specified API endpoint.
            fetch_data() : Fetches a subset of data from the specified API endpoint based on offset and limit.

        Parameters :
            logger       : A logger Object to log informational messages and errors.
        """

        self.logger = logger
        self.access_token = None
        self.expires_at = 0

    def fetch_response(self, url, auth=None, params=None, headers=None, max_retries=3) :
        """
        Fetch the data from the API.

        Parameters:
            url(object)        : The API endpoint URL.
            auth (connection)  : authentication details.
            offset (int)       : The starting point for data retrieval.
            limit (int)        : The number of records to fetch.
            params (dict)      : Optional parameters to include in the request.
            headers (dict)     : Optional HTTP headers.
            max_retries        : Maximum number of retries in case of failure.

        Returns:
            List[Dicts]  : The data fetched from the API.
        """
        self.logger.info('Executing fetch_data module in class ServiceNow_DataFetcher')
        retry_count = 0

        while retry_count <= max_retries:
            try:
                response = requests.get(url, auth=auth, params=params, headers=headers)
                response.raise_for_status()
                data = response.json()
                if "error" in data:
                    raise Exception ("Incomplete data in response")
                results = data.get('result', [])
                self.logger.info(f'Data fetched from API : Success')
                return results

            except Exception as e:
                self.logger.error(f'Failed to fetch data from API : {e}.')
                self.logger.info(f'Retrying to fetch data . Retry count: {retry_count}')
                retry_count += 1
                time.sleep(2 ** retry_count)

                if retry_count > max_retries:
                    self.logger.error(f'Data fetching from API : Failed after {max_retries} retries.')
                    if 'result' in data :
                        return data['result']
                    return []  # Return empty list on exception

    def generate_access_token(self, src_cnct_path, src_environment, authenticator_type):
        """
        Ensures a valid access token is available by checking if the current token is valid or needs to be refreshed.

        Args:
            config (dict): Configuration dictionary containing:
                - 'src_cnct_path': Path to the credentials or connection profile.
                - 'src_environment': The environment or connection profile.
                - 'authenticator_type': The type of authenticator to use.

        Returns:
            str: The current valid access token.
        """

        try :
            self.logger.info('Executing generate_access_token from class ServiceNow_DataFetcher')
            # Fetch access token only if no token or expired
            if not self.access_token or time.time() >= (self.expires_at-300):
                client = ServiceNow_Authenticator(logger= self.logger, creds_path= src_cnct_path, src_conn_profile= src_environment)
                token_info = client.connect(authenticator_type = authenticator_type)
                self.access_token = token_info['access_token']
                self.expires_at = token_info['expires_at']
                self.logger.info(f'Access Token generation : success')
                self.logger.info(f'Current time: {time.time()}, Token Expires at: {self.expires_at}')

            return self.access_token
        except Exception as e:
            self.logger.info('Failure while Executing generate_access_token from class ServiceNow_DataFetcher')
            self.logger.info(f'Access Token generation : Failure : Error -> {e}')
            raise

    def get_max_date_from_redshift(self, config_path, profile, schema, table, orderby_col):
        """
        Retrieves the maximum value from a specified column in a Redshift table.

        Parameters:
            config_path (str): Path to the configuration file.
            profile (str): Profile name for the Redshift connection.
            schema (str): Schema name where the table resides.
            table (str): Table name from which to fetch the maximum value.
            orderby_col (str): Column name to retrieve the maximum value from.
            logger (logging.Logger): Logger instance for logging.

        Returns:
            datetime: The maximum value from the specified column. Returns `None` if the column is empty.

        Raises:
            Exception: If there is an error during the database query.
        """
        try :
            self.logger.info(f'Executing get_max_date_from_redshift module from class ServiceNow_DataFetcher to get Max Value')
            engine = get_connection(config_path, profile, self.logger)
            result = engine.execute(f'SELECT MAX({orderby_col}) from {schema}.{table} limit 1;')
            result = result.first()[0]
            self.logger.info(f'Max {orderby_col} fetched from database : {result}')
            return result
        except Exception as e:
            self.logger.info(f'Failure while Executing get_max_date_from_redshift module from class ServiceNow_DataFetcher')
            self.logger.info(f'Exception while fetching the max date from Database : Error : {e}')
            raise

    def check_columns(self, df, expected_columns):
        df_columns = df.columns.tolist()

        required_set = set(expected_columns)
        actual_set = set(df_columns)

        # Determine missing and extra columns
        missing_columns = list(required_set - actual_set)
        extra_columns = list(actual_set - required_set)

        return not missing_columns and not extra_columns, missing_columns, extra_columns


    def fetch_api_data(self, url, src_cnct_path, src_environment, limit, batch_size, authenticator_type=None, offset=0, auth=None, headers=None, params=None):
        """
        Fetches data from API in chunks and returns data in batches. Handles pagination, data accumulation, and batch loading.

        Parameters:
            url (str): Base URL for the API endpoint.
            src_cnct_path (str): Path for the source connection.
            src_environment (str): Environment for the source.
            limit (int): Number of records to fetch per API request.
            batch_size (int): Number of records to accumulate before returning.
            offset (int, optional): Offset for pagination. Defaults to 0.
            authenticator_type (Optional): Type of authentication.
            auth (optional): Authentication credentials.
            params (dict, optional): Additional parameters to include in the API request.

        Returns:
            bool: `Final_df` Dataframe is returned

        Raises:
            Exception: If there is an error during data fetching or loading.
        """

        # To fetch total api data
        try :
            self.logger.info(f'Executing fetch_api_data_and_load module from class ServiceNow_DataFetcher')
            Final_df = pd.DataFrame()   #Intialzing the empty dataframe
            total_fetch_count = 0
            data = True

            while data :
                if authenticator_type:
                    headers = {'Authorization': f'Bearer {self.generate_access_token(src_cnct_path, src_environment, authenticator_type)}'}
                main_url = f'{url}?sysparm_offset={offset}&sysparm_limit={limit}'
                data = self.fetch_response(url=main_url, auth=auth, headers=headers, params=params)
                if not data :  #Break the loop if no data is returned
                    self.logger.info(f'No more data to fetch. Ending pagination.')
                    break

                df = pd.DataFrame(data, dtype=object)  #load each result into dataframe
                self.logger.info(f'Data is fetched from api : {offset} to {offset+limit}')
                Final_df = pd.concat([Final_df,df])
                offset += limit
                total_fetch_count += len(df)
                del df      #delete df after loading to final df for memory optimizatiion

                if len(Final_df) >= batch_size :
                    return Final_df,offset

            return Final_df,offset

        except Exception as e:
            self.logger.info(f'Failure while Executing fetch_api_data_and_load module from class ServiceNow_DataFetcher')
            self.logger.info(f'Exception while loading data from API to Database : Error : {e}')
            raise

    def assignmentGroupID_fetcher(self,assignment_group_names,assignment_group_url,auth,headers=None):
        """
        Get assignment group ids for list of assignment group names

        Parameters:
            assinment_group_names : List of assignment group names passed in config file
            assignment_group_url : The API endpoint URL for assignment group servicenow api table
            headers : Optional HTTP headers
            auth : Authentication to acces the api
            headers : Optional HTTP headers

        Returns:
           list: list of assignment group ids fetched from assignment group url
        """
        try:
            self.logger.info(f"working on fetching assignment group sys_id for {assignment_group_names}")
            assignment_groups = ','.join(assignment_group_names)
            params = {
                    "sysparm_query" : f"nameIN{assignment_groups}"
            }
            response = requests.get(assignment_group_url,auth=auth,params=params,headers=headers)
            response.raise_for_status()
            data=response.json()
            assignmentsGroup_id_response = data["result"]
            assignmentGroup_id = []
            for value in assignmentsGroup_id_response:
                assignmentGroup_id.append(value["sys_id"])
            self.logger.info(f"Assignment group sysid has been sucessfully fetched -- > Id : {assignmentGroup_id}")
            return assignmentGroup_id

        except Exception as e:
            self.logger.error(f"failed to fetch assignment group ids error : {e}")
            raise

    def check_subapi_values(self,itemkey,item):
       """
       This function will returns sub values of item (dict)

       """
       try:
            value = item.get(itemkey).get('value') if isinstance(item.get(itemkey), dict) and 'value' in item[itemkey] else ''
            return value
       except Exception as e:
           self.logger.error(f"Failed while fetching sub values error : {e}")
           raise

    def DataFetcher_from_main_api_result(self,result,direct_columns,subapi_columns):
      """
      Read and load the data into a Dataframe for required columns from incident raw json data

      Parameters :
             result : json raw data returned from fetch_data()
             direct_columns : list of columns thats need to fetch from main api
             subapi_columns(optional) : list of columns thats need to fetch from sub api. It is optional if we don't have any sub_api columns don't pass any values

      Return :
           Dataframe : Returns Dataframe
      """
      try:
        df = pd.DataFrame()
        for item in result:
                new_row={}
                for col in direct_columns:
                    new_row[col]=item.get(col)

                for col in subapi_columns:
                    new_row[f"{col}_sys_id"]= self.check_subapi_values(col,item)
                new_row_df = pd.DataFrame(new_row,index=[0])
                df = pd.concat([df,new_row_df],ignore_index=True)
        return df
      except Exception as e:
          self.logger.error(f"Failed data reading of incident details raw json due to error:{e} ")
          raise

    def subtablesDetais_fetcher(self,incident_df,subapi_columns_details,api_mainurl,auth):
            """
            This menthod will fetch the data for sub api tables which has been passed in config file and subapi data df will merges with main df and also handling audit columns

            Parameters :
                incident_df : Main Dataframe that has been fetched from incident table
                subapi_columns_details : Sub api detais (i.e subapi tables,columns)
                schema_name : target schema name
                posting_agent : .py file name

            Return :
                 Dataframe
            """
            try:
                self.logger.info("Working on fetching subapi details......")
                incident_add_df = incident_df
                for key, item in subapi_columns_details.items():
                    primary_key = f"{key}_sys_id"
                    sub_data_df = self.subapi_caller(list(set(incident_df[primary_key].to_list())),item["api_table_name"],item["subapi_internal_columns"],key,api_mainurl,auth)
                    incident_add_df = incident_add_df.merge(sub_data_df, on=primary_key,how='left')
                final_df=incident_add_df.assign(load_dtm = datetime.now())
                return final_df
            except Exception as e:
                self.logger.error(f"Failed while fetching subapi values error : {e}")
                raise

    def subapiDataFetcher(self,chunk,tablename,columns,key,api_mainurl,auth):
      """
      This function will takes main df values in chunks and fetches vales form the subapi items

      Parameters :
             chunk : 100 values size of chunk
             tablename : subapi table name
             columns : Column names that needs to fetch from subapi
             key : Subapi main column name that needs to assign as prefix for every subapi column names
      """
      sub_api_url=api_mainurl.rsplit('/',1)[0]
      string_ids = ','.join(chunk)
      response = requests.get(f"{sub_api_url}/{tablename}?sysparm_query=sys_idIN{string_ids}",auth=auth).json()["result"]
      df = pd.DataFrame()
      for res in response:
            new_row = {}
            for col in columns:
                   new_row[f"{key}_{col}"] = res.get(col)
            sub_table_df = pd.DataFrame(new_row,index=[0])
            df = pd.concat([df,sub_table_df],ignore_index=True)
      return df

    def subapi_caller(self,column_values,tablename,columns,key,api_mainurl,auth):
            """
            This function will takes input of subapi sys_ids and fetches the sub api columns data into a Dataframe

            Parameters :
                column_values : Columns values that are listing from incident Dataframe
                tablename : subapi table name
                columns : Column names that needs to fetch from subapi
                key : Subapi main column name that needs to assign as prefix for every subapi column names
            """
            try:
                if None in column_values:
                    column_values.remove('')
                else:
                    column_values
                chunk_size = 100
                df = pd.DataFrame()
                for i in range(0,len(column_values),chunk_size):
                        chunk = column_values[i:i+chunk_size]
                        df1 = self.subapiDataFetcher(chunk,tablename,columns,key,api_mainurl,auth)
                        df = pd.concat([df,df1],ignore_index=True)
                return df
            except Exception as e:
                self.logger.error(f"failed running subapi_caller function error : {e}")

    def remove_columns(self,final_df,drop_columns):
        """
            Removes unwanted columns from Dataframe
        Parameters:
            final_df: Datframe
            drop_columns: List of columns to drop from Dataframe
        Return:
        Returns dataframe
        """
        final_df = final_df.drop(columns=drop_columns)
        self.logger.info("columns has been dropped from Dataframe")
        return final_df


    def incidentDetails_fetcher(self,assignment_group_sys_ids,api_mainurl,auth,incident_columns,subapi_columns,headers=None,params=None):
        """
        Hits the api based and fetches the data from json data

        Parameters:
            assignment_group_sys_ids : list of assignment group ids, to fetch incident details
            api_mainurl : Api url
            auth : authentication
            incident_columns: list of columns that need to fetch main api(incident) details
            subapi_columns : list of sub api columns that need to fetch main api(incident) details
        Returns:
            Returns Dataframe
        """
        self.logger.info("Working on fetcing incident details")
        assignment_group_sys_id_filter = ','.join(assignment_group_sys_ids)
        data = True
        sysparm_offset_value = 0
        page_count = 0
        sysparm_limit = 10000
        df = pd.DataFrame()
        while data:
                '''default_params = {
                    "sysparm_query" : f"assignment_groupIN{assignment_group_sys_id_filter}",
                    "sysparm_offset" : {sysparm_offset_value},
                    "sysparm_limit"   : {sysparm_limit}
                    }'''
                mainurl = f"{api_mainurl}?sysparm_query=assignment_groupIN{assignment_group_sys_id_filter}&sysparm_offset={sysparm_offset_value}&sysparm_limit={sysparm_limit}"
                print(mainurl)
                data = self.fetch_response(url=mainurl,auth=auth)
                #response = requests.get(api_mainurl,auth=HTTPBasicAuth(username,password),params=None)
                #data = response.json()["result"]
                df1 = self.DataFetcher_from_main_api_result(data,incident_columns,subapi_columns)
                df = pd.concat([df,df1],ignore_index=True)
                sysparm_offset_value +=10000
                page_count +=1
        self.logger.info(f"Total no of pages scrolled : {page_count}")
        self.logger.info("Incident details has been fetched sucessfully")
        return df


