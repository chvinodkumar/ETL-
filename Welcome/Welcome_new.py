for col_name, sqlalchemy_type in table_dtypes.items():
    if col_name in data.columns:  # Check if the column exists in the DataFrame
        data[col_name] = data[col_name].replace("", pd.NA)
        if expected_dtype and df_dtypes[col_name] != expected_dtype:
            if expected_dtype == 'bool':
                data[col_name] = data[col_name].replace({'True': True, 'False': False})
                data[col_name] = data[col_name].astype(pd.BooleanDtype())
            else:
                data[col_name] = data[col_name].astype(expected_dtype)
    else:
        self.logger.warning(f"Column '{col_name}' not found in the data. Skipping transformation for this column.")



            for col_name, sqlalchemy_type in table_dtypes.items():
                expected_dtype = sqlalchemy_to_pandas_dtype.get(type(sqlalchemy_type), str)
                data[col_name]=data[col_name].replace("",pd.NA)
                if expected_dtype and df_dtypes[col_name] != expected_dtype:
                    if expected_dtype=='bool':
                        data[col_name] = data[col_name].replace({'True': True, 'False': False})
                        data[col_name] = data[col_name].astype(pd.BooleanDtype())
                    else:
                        data[col_name] = data[col_name].astype(expected_dtype)
            self.logger.info(f"transform method executed successfully - Returning data by checking for data types after doing any casting if required")
            return data