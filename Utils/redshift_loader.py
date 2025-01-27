from sqlalchemy import create_engine, MetaData, Table, INTEGER, BOOLEAN, TIMESTAMP, DATETIME, DATE, FLOAT,String,types
from sqlalchemy.orm import sessionmaker
import configparser
import pandas as pd
from redshift_connector import get_connection

class Database:
    """
    A class Database which performs Truncate, Insert, and Upsert/Incremental operations on the provided tables
    """
    def __init__(self, logger, config, profile, data, load_type, schema, main_table_name, stage_table_name=None, primary_key=None,log_table_primary_key=None,orderby_col=None,log_table=None):
        """
        The constructor for Database class

        Parameters:
        logger (object)        : Logger object where log entries are to be made
        config (str)           : Path of redshift credentials
        profile (str)          : Redshift profile
        data (DataFrame)       : DataFrame constructed from the response fetched
        load_type (str)        : truncate_and_load / incremental / fullload - input based on the requirement
        schema (str)           : Schema name
        main_table_name (str)  : Main target table name
        stage_table_name (str) : Target Stage table name required if incremental load type or by default it is None
        primary_key (str)      : Primary Key in the table if incremental load type or by default it is None
        orderby_col (str)      : orderby_col in the table if remove_duplicates_and_load load type or by default it is None
        log_table_primary_key(str) : Primark key in the log table if soft_deletes load type or by default it is None
        log_table (str)        : log table name
        """
        self.logger = logger
        self.logger.info("Running Database Module")
        self.main_table = main_table_name
        self.stage_table = stage_table_name
        self.data = data
        self.primary_key = primary_key
        self.orderby_col = orderby_col
        self.load_type = load_type
        self.engine = get_connection(config, profile, logger)
        self.metadata = MetaData(bind=self.engine, schema=schema)
        self.Session = sessionmaker(bind=self.engine)
        self.schema=schema
        self.log_table_primary_key = log_table_primary_key
        self.log_table = log_table
        self.initiate_load()
        self.close()

    def initiate_load(self):
        """
        A method to initiate type of load method based on the inputs received

        Parameters: None
        Returns: None
        """
        try:
            if self.load_type == "truncate_and_load":
                self.logger.info("Proceeding with truncate and load")
                self.truncate_table(self.main_table)
                self.insert_data(self.main_table, self.data)
            elif self.load_type == "fullload":
                self.logger.info("Proceeding with append only")
                self.insert_data(self.main_table, self.data)
            elif self.load_type == "incremental":
                self.logger.info("Proceeding with incremental load")
                self.truncate_table(self.stage_table)
                self.insert_data(self.stage_table, self.data)
                self.incremental_load(self.main_table, self.stage_table, self.primary_key)
            elif self.load_type == "remove_duplicates_and_load":
                self.logger.info("Proceeding with remove duplicate_and_load load load to main table")
                self.drop_duplicates(self.stage_table,self.primary_key,self.orderby_col)
                self.incremental_load(self.main_table, self.stage_table, self.primary_key)
            elif self.load_type =="log_based_soft_deletes":
                self.logger.info("Proceeding with soft_deletes load")
                self.log_based_soft_deletes(self.main_table, self.log_table, self.primary_key, self.log_table_primary_key, self.schema)
        except Exception as e:
            self.logger.error(f"initiate_load method execution failed with error --> {e}")
            raise

    def transform(self, data, table):
        """
        A method to perform Data Type transformations if not matched with DataFrame and table created in DB

        Parameters:
        data (DataFrame) : Pandas DataFrame constructed from any type (csv,excel,parquet,text)
        table (object)   : Object of the table

        Returns:
        data (DataFrame) : Pandas DataFrame which has undergone Data Type casting
        """
        self.logger.info(f"Executing transform method in Database class to perform type casting if required for {table}")
        try:
            df_dtypes = data.dtypes.to_dict()
            table_dtypes = {col.name: col.type for col in table.columns}
            sqlalchemy_to_pandas_dtype = {
                INTEGER: 'int64',
                BOOLEAN: 'bool',
                TIMESTAMP: 'datetime64[ns]',
                DATETIME: 'datetime64[ns]',
                DATE: 'datetime64[ns]',
                FLOAT: 'float64',
                String : 'object'

            }
            for col_name, sqlalchemy_type in table_dtypes.items():
                expected_dtype = sqlalchemy_to_pandas_dtype.get(type(sqlalchemy_type), 'object')
                if expected_dtype and df_dtypes[col_name] != expected_dtype:
                    data[col_name] = data[col_name].astype(expected_dtype)
            self.logger.info(f"transform method executed successfully - Returning data by checking for data types after doing any casting if required")
            return data
        except Exception as e:
            self.logger.error(f"Failed to execute transform method in Database class. Type casting failed, error --> {e}")
            raise

    def close(self):
        """
        A method to close the DB connection

        Parameters : None
        Returns : None
        """
        self.logger.info("Closing DB connection by executing close method in Database class")
        try:
            self.Session.close_all()
            self.engine.close()
            self.logger.info("DB Connections closed")
        except Exception as e:
            self.logger.error(f"Failed to execute close method in Database class. DB connection was not closed, error --> {e}")
            raise

    def truncate_table(self, table_name: str):
        """
        A method to execute truncate operation on the table provided as input

        Parameters:
        table_name (str) : Name of the table which data has to be truncated

        Returns : None
        """
        self.logger.info(f"Executing truncate_table method in Database class for {table_name}")
        try:
            table = Table(table_name, self.metadata, autoload_with=self.engine)
            with self.Session() as session:
                session.execute(table.delete())
                self.logger.info(f"{table_name} has been truncated")
                session.commit()
                self.logger.info("Truncate operation has been committed")
                self.logger.info(f"truncate_table method executed successfully - {table_name} has been truncated")
        except Exception as e:
            self.logger.error(f"Failed to execute truncate_table method in Database class for {table_name}, error --> {e}")
            raise

    @staticmethod
    def sqlcol(dfparam):
        """
        A static method to enable collation and input object as varchar values while performing db insert

        Parameters:
        dfparam (DataFrame) : DataFrame which has to undergo this change and will be inserted to DB

        Returns : None
        """
        dtypedict = {}
        for i, j in zip(dfparam.columns, dfparam.dtypes):
            if "object" in str(j):
                dtypedict.update({i: types.VARCHAR(collation='case_insensitive')})
        return dtypedict

    def insert_data(self, table_name, data):
        """
        A method to execute insert operation on the table provided as input with data

        Parameters:
        table_name (str) : Name of the table which data has to be inserted
        data (DataFrame) : Pandas DataFrame constructed from any type (csv,excel,parquet,text)

        Returns : None
        """
        self.logger.info(f"Executing insert_data method in Database class for {table_name}")
        try:
            table = Table(table_name, self.metadata, autoload_with=self.engine)
            transformed_data = self.transform(data, table)
            transformed_data.to_sql(name=table_name, schema=self.schema,con=self.engine, if_exists='append', index=False, chunksize=10000,method='multi',dtype=self.sqlcol(transformed_data))
            self.logger.info(f"Insert into {table_name} completed - insert_data method executed successfully")
        except Exception as e:
            self.logger.error(f"Failed to execute insert_data method in Database class for {table_name}, error --> {e}")
            raise

    def incremental_load(self, main_table, stage_table, primary_key):
        """
        A method to execute upsert/incremental operations on the tables provided as input

        Parameters:
        main_table (str)  : Name of the main table to which incremental records have to be inserted
        stage_table (str) : Name of the table which has incremental data pulled from source
        primary_key (str) : Primary Key column which is present in both main_table and stage_table

        Returns : None
        """
        self.logger.info(f"Executing incremental_load method in Database class for main table {main_table} and stage table {stage_table}")
        try:
            delete_query = f"""
            DELETE FROM {self.schema}.{main_table} USING {self.schema}.{stage_table}
            WHERE {self.schema}.{main_table}.{primary_key} = {self.schema}.{stage_table}.{primary_key}
            """
            insert_query = f"""
            INSERT INTO {self.schema}.{main_table}
            SELECT * FROM {self.schema}.{stage_table}
            """
            self.engine.execute(delete_query)
            self.logger.info(f"Deleted records in main table {main_table} matching with the ones in stage table {stage_table}")
            self.engine.execute(insert_query)
            self.logger.info(f"Inserted incremental records in main table {main_table}")
            self.logger.info(f"incremental_load method executed successfully for {stage_table} and {main_table}")
        except Exception as e:
            self.logger.error(f"Failed to execute incremental_load method in Database class for main table {main_table} & stage table {stage_table}, error --> {e}")
            raise

    def drop_duplicates(self, stage_table, primary_key, orderby_col) :
        try :
            self.logger.info(f"Executing drop_duplicates method in Database class for stage table {stage_table}")
            create_temp_table_query = f"""
            create table {self.schema}.{stage_table}_temp as
            SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY {primary_key} ORDER BY {orderby_col} DESC) AS row_num
            FROM {self.schema}.{stage_table})
            """
            duplicate_delete_query = f"""
            DELETE FROM {self.schema}.{stage_table}_temp
            where row_num > 1
            """
            drop_row_num_column_query = f"""
            Alter table {self.schema}.{stage_table}_temp drop column row_num
            """
            drop_and_rename_query = f"""
            drop table {self.schema}.{stage_table};
            Alter table {self.schema}.{stage_table}_temp rename to {stage_table}
            """
            self.engine.execute(create_temp_table_query)
            self.logger.info(f"Created an {stage_table}_temp with row number column")
            r=self.engine.execute(duplicate_delete_query)
            self.logger.info(f"No of Duplicates removed : {r.rowcount}")
            self.logger.info(f"Removed the duplicates from {stage_table}_temp table")
            self.engine.execute(drop_row_num_column_query)
            self.logger.info(f"Dropped the row number column from {stage_table}_temp table")
            self.engine.execute(drop_and_rename_query)
            self.logger.info(f"Rename the {stage_table}_temp to {stage_table}")
        except Exception as e:
            self.logger.error(f"Failed to execute drop_duplicates method in Database class for stage table {stage_table}, error --> {e}")
            raise

    def log_based_soft_deletes(self,main_table,log_table,primary_key,log_table_primary_key,schema):
        """
        A method is to handle the soft deletes on main table with refrence of the log table

        Parameters:

        main_table(str) : Name of the table to handle soft deletes
        log_table(str) : Name of the log table that refers main table to handle soft deletes
        primary_key(str) : Primary key column from main table
        log_table_primary_key(str) : Primary key column from log table
        Schema(str) : Schema name of the tables
        """
        self.logger.info(f"Executing soft_deletes method for main table {main_table}")
        try:
            soft_deletes_query = f"""
                UPDATE {schema}.{main_table}
                SET is_deleted = 1
                WHERE is_deleted = 0
                AND EXISTS (
                   SELECT 1
                   FROM {schema}.{log_table} log
                   WHERE log.{log_table_primary_key} = {schema}.{main_table}.{primary_key})"""
            self.logger.info(f"Soft_delete script executed sucessfully for {main_table}")
            results = self.engine.execute(soft_deletes_query)
            no_rows_updated = results.rowcount
            self.logger.info(f"Toatal no of records has been processed : {no_rows_updated}")
            print(f"Toatal no of records has been processed : {no_rows_updated}")

        except Exception as e:
            self.logger.info(f"failed to exceute soft_deletes method in Database class for main table {main_table}, error -->{e}")
            raise


