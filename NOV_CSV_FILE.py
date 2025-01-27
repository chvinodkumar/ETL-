import pandas as pd
import logging
import os
from datetime import datetime

# Setup logger
logging.basicConfig(
    filename='validation.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'  # This will overwrite the file each time the script is run
)


# class DataValidation:
#     def __init__(self, source_file) -> None:
#         self.src_file_nm = "NOV_CSV_FILE.py"
#         self.load_data()

#         # Start validation process
#         # self.validate()

#     def load_data(self):
#         """Load all necessary data files."""
#         try:
#             self.source_file_df = pd.read_excel(self.src_file_nm).convert_dtypes()
#             self.source_file_df['validation_status'] = ''

            # self.manufacture_lkp_df = pd.read_csv('manufac.csv', encoding='ISO-8859-1')
            # self.manufacture_lkp_df['validation_status'] = ''
    #
    #         self.rule_id_df = pd.read_csv('rule_id.csv').convert_dtypes()
    #         self.rule_mapping_df = pd.read_csv('rule_mapping.csv').convert_dtypes()
    #         self.column_rules = self.rule_mapping_df.merge(self.rule_id_df, on='Rule_id').to_dict(orient='records')
    #
    #         # Initialize audit log with audit_file_names
    #         self.DQ_jobRun = {
    #             'rundate': datetime.now().strftime('%Y-%m-%d'),
    #             'start_time': datetime.now()
    #         }
    #         self.DQ_jobRun_Result = {
    #             'rundate': datetime.now().strftime('%Y-%m-%d'),
    #             'start_time': datetime.now()
    #         }
    #         self.Rept_DQ_jobRun_Result = {
    #             'rundate': datetime.now().strftime('%Y-%m-%d'),
    #             'start_time': datetime.now()
    #         }
    #
    #         # Counters for passed and failed records
    #         self.passed_count = 0
    #         self.failed_count = 0
    #         self.error_file = 'error_file.csv'
    #         self.lkp_error_file = 'lkp_error_file.csv'
    #
    #         logging.info("Data files loaded successfully.")
    #     except FileNotFoundError as e:
    #         logging.error(f"File not found: {e}")
    #         raise
    #     except Exception as e:
    #         logging.error(f"Error loading data files: {e}")
    #         raise
    #
    # def validate(self):
    #     rules = {
    #         1: self.rule_1,
    #         2: self.rule_2,
    #         3: self.rule_3,
    #         4: self.rule_4,
    #         5: self.rule_5,
    #         6: self.rule_6,
    #         7: self.rule_7,
    #         8: self.rule_8,
    #         9: self.rule_9,
    #         10: self.rule_10,
    #         11: self.rule_11,
    #         12: self.rule_12,
    #         13: self.rule_13,
    #         14: self.rule_14,
    #         15: self.rule_15,
    #         16: self.rule_16,
    #
    #     }
    #
    #     for val in self.column_rules:
    #         rule_id = val.get('Rule_id')
    #         rule_func = rules.get(rule_id)
    #
    #         if rule_func:
    #             try:
    #                 logging.info(f"Applying Rule {rule_id} on {val['column_name']}")
    #                 rule_func(val)
    #                 self.passed_count += 1  # Increment passed count
    #             except Exception as e:
    #                 self.failed_count += 1  # Increment failed count
    #                 logging.error(f"Error applying Rule {rule_id}: {e}")
    #         else:
    #             logging.warning(f"Rule {rule_id} is not defined.")
    #
    #     # Write audit log to CSV
    #     # self.write_audit_log_to_csv()
    #     self.DQ_jobRun_log_to_csv()
    #     self.DQ_jobRun_Result_log_to_csv()
    #     self.Rept_DQ_jobRun_Result_log_to_csv()
    #     self.source_file_df.to_csv(self.error_file, index=False)
    #     self.manufacture_lkp_df.to_csv(self.lkp_error_file, index=False)
    #
    # # def write_audit_log_to_csv(self):
    # #     """Write the audit log to a CSV file using pandas."""
    # #     total_records = self.passed_count + self.failed_count
    # #     self.audit_log['status'] = f'{(self.passed_count / total_records * 100):.2f}% Passed' if total_records > 0 else 'No Records'
    # #     self.audit_log['rule_passed_count'] = self.passed_count
    # #     self.audit_log['rule_failed_count'] = self.failed_count
    # #     self.audit_log['src_file_name'] = self.src_file_nm
    # #     self.audit_log['end_time'] = datetime.now()
    # #     pd.DataFrame([self.audit_log]).to_csv('audit_log.csv', index=False, encoding='utf-8')
    # #     logging.info(f"Audit log written to audit_log.csv.")
    #
    # def DQ_jobRun_log_to_csv(self):
    #     """Write the audit log to a CSV file using pandas."""
    #     total_records = self.passed_count + self.failed_count
    #     self.DQ_jobRun[
    #         'status'] = f'{(self.passed_count / total_records * 100):.2f}% Passed' if total_records > 0 else 'No Records'
    #     self.DQ_jobRun['rule_passed_count'] = self.passed_count
    #     self.DQ_jobRun['rule_failed_count'] = self.failed_count
    #     self.DQ_jobRun['src_file_name'] = self.src_file_nm
    #     self.DQ_jobRun['end_time'] = datetime.now()
    #     pd.DataFrame([self.DQ_jobRun]).to_csv('DQ_jobRun.csv', index=False, encoding='utf-8')
    #     logging.info(f"Audit log written to DQ_jobRun.csv.")
    #
    # def DQ_jobRun_Result_log_to_csv(self):
    #     """Write the audit log to a CSV file using pandas."""
    #     total_records = self.passed_count + self.failed_count
    #     self.DQ_jobRun_Result[
    #         'status'] = f'{(self.passed_count / total_records * 100):.2f}% Passed' if total_records > 0 else 'No Records'
    #     self.DQ_jobRun_Result['rule_passed_count'] = self.passed_count
    #     self.DQ_jobRun_Result['rule_failed_count'] = self.failed_count
    #     self.DQ_jobRun_Result['src_file_name'] = self.src_file_nm
    #     self.DQ_jobRun_Result['end_time'] = datetime.now()
    #     pd.DataFrame([self.DQ_jobRun_Result]).to_csv('DQ_jobRun_Result.csv', index=False, encoding='utf-8')
    #     logging.info(f"Audit log written to DQ_jobRun_Result.csv.")
    #
    # def Rept_DQ_jobRun_Result_log_to_csv(self):
    #     """Write the audit log to a CSV file using pandas."""
    #     total_records = self.passed_count + self.failed_count
    #     self.Rept_DQ_jobRun_Result[
    #         'status'] = f'{(self.passed_count / total_records * 100):.2f}% Passed' if total_records > 0 else 'No Records'
    #     self.Rept_DQ_jobRun_Result['rule_passed_count'] = self.passed_count
    #     self.Rept_DQ_jobRun_Result['rule_failed_count'] = self.failed_count
    #     self.Rept_DQ_jobRun_Result['src_file_name'] = self.src_file_nm
    #     self.Rept_DQ_jobRun_Result['end_time'] = datetime.now()
    #     pd.DataFrame([self.Rept_DQ_jobRun_Result]).to_csv('Rept_DQ_jobRun_Result.csv', index=False, encoding='utf-8')
    #     logging.info(f"Audit log written to Rept_DQ_jobRun_Result.csv.")
    #
    # def update_validation_status_by_index(self, df, failed_rows_index, message):
    #     # For each row, if 'validation_status' is already populated, append the message
    #     for idx in failed_rows_index:
    #         current_status = df.at[idx, 'validation_status']
    #         if pd.notna(current_status) and current_status != "":
    #             df.at[idx, 'validation_status'] = f"{current_status} | {message}"
    #         else:
    #             df.at[idx, 'validation_status'] = message
    #     return df
    #
    # def rule_1(self, val):
    #     """Ensure the column values are numeric."""
    #     column_name = val['column_name']
    #     failed_rows = self.source_file_df[
    #         ~self.source_file_df[ource_file_dfcolumn_name].apply(lambda x: isinstance(x, (int, float)))]
    #     self.update_validation_status_by_index(self.source_file_df, failed_rows.index, f"{column_name}-1-Fail")
    #
    # def rule_2(self, val):
    #     """Ensure the column values are not blank."""
    #     column_name = val['column_name']
    #     failed_rows = self.source_file_df[
    #         self.source_file_df[column_name].isnull() | (self.source_file_df[column_name] == '')]
    #     self.update_validation_status_by_index(self.source_file_df, failed_rows.index, f"{column_name}-2-Fail")
    #
    # def rule_3(self, val):
    #     """Ensure the column values are unique."""
    #     column_name = val['column_name']
    #     duplicates = self.source_file_df[self.source_file_df.duplicated(subset=[column_name], keep=False)]
    #     self.update_validation_status_by_index(self.source_file_df, duplicates.index, f"{column_name}-3-Fail")
    #
    # def rule_4(self, val):
    #     """Count of distinct column values should be <= 1."""
    #     column_name = val['column_name']
    #     distinct_count = self.source_file_df[column_name].nunique()
    #     if distinct_count >= 1:
    #         raise ValueError(f"Count of distinct values in column {column_name} cannot be greater than 1.")
    #
    # def rule_5(self, val):
    #     """Column cannot be blank/null."""
    #     column_name = val['column_name']
    #     failed_rows = self.source_file_df[self.source_file_df[column_name].isnull()]
    #     self.update_validation_status_by_index(self.source_file_df, failed_rows.index, f"{column_name}-5-Fail")
    #
    # def rule_6(self, val):
    #     """Ensure unique serial number once equipment has a startup date."""
    #     startup_date_col = 'STARTUP_DATE'  # Replace with actual column name
    #     serial_number_col = 'SERIAL_NUMBER'  # Replace with actual column name
    #     df_with_startup = self.source_file_df[self.source_file_df[startup_date_col].notnull()]
    #     duplicates = df_with_startup[df_with_startup[serial_number_col].duplicated(keep=False)]
    #     self.update_validation_status_by_index(self.source_file_df, duplicates.index, f"{val['column_name']}-5-Fail")
    #
    # def rule_7(self, val):
    #     """Column cannot be blank/null for lookup table."""
    #     column_name = val['column_name']
    #     failed_rows = self.manufacture_lkp_df[self.manufacture_lkp_df[column_name].isnull()]
    #     self.update_validation_status_by_index(self.manufacture_lkp_df, failed_rows.index, f"{column_name}-7-Fail")
    #
    # def rule_8(self, val):
    #     """Ensure manufacturer and model match lookup."""
    #     column_name = val['column_name']
    #     merged_df = pd.merge(self.source_file_df, self.manufacture_lkp_df, on=['MANUFACTURER_NAME', 'MODEL_NAME'],
    #                          suffixes=('_src', '_lkp'))
    #     mismatched_rows = merged_df[merged_df[f'{column_name}_src'] != merged_df[f'{column_name}_lkp']]
    #     self.update_validation_status_by_index(self.source_file_df, mismatched_rows.index, f"{column_name}-8-Fail")
    #
    # def rule_9(self, val):
    #     column_name = val['column_name']
    #     merged_df = pd.merge(self.source_file_df, self.manufacture_lkp_df, on=['MANUFACTURER_NAME', 'MODEL_NAME'],
    #                          suffixes=('_src', '_lkp'))
    #     mismatched_rows = merged_df[merged_df[f'{column_name}_src'] <= merged_df[f'{column_name}_lkp']]
    #     self.update_validation_status_by_index(self.source_file_df, mismatched_rows.index, f"{column_name}-9-Fail")
    #
    # def rule_10(self, val):
    #     """Ensure the column values are non-negative."""
    #     column_name = val['column_name']
    #     # Identify rows where the values are negative
    #     failed_rows = self.source_file_df[self.source_file_df[column_name] < 0]
    #     # Update validation status for those rows
    #     self.update_validation_status_by_index(self.source_file_df, failed_rows.index, f"{column_name}-10-Fail")
    #
    # def rule_11(self, val):
    #     column_name = val['column_name']
    #     invalid_rows = self.source_file_df[self.source_file_df[column_name] < 0]
    #     self.update_validation_status_by_index(self.source_file_df, invalid_rows.index, f"{column_name}-11-Fail")
    #
    # def rule_12(self, val):
    #     raise ValueError()
    #
    # def rule_13(self, val):
    #     """Check if FUEL_HHV is not less than FUEL_LHV and flag rows that fail."""
    #     rule_condition = self.manufacture_lkp_df['FUEL_HHV'] < self.manufacture_lkp_df['FUEL_LHV']
    #     self.update_validation_status_by_index(self.manufacture_lkp_df, rule_condition.index,
    #                                            f"{val['column_name']}-2-Fail")
    #
    # def rule_14(self, val):
    #     column_name = val['column_name']
    #     invalid_rows = self.source_file_df[self.source_file_df[column_name] < 0]
    #     self.update_validation_status_by_index(self.source_file_df, invalid_rows.index, f"{column_name}-14-Fail")
    #
    # def rule_15(self, val):
    #     raise ValueError()
    #
    # def rule_16(self, val):
    #     raise ValueError()
    #
    # def rule_17(self, val):
    #     raise ValueError()
    #
    # def rule_18(self, val):
    #     raise ValueError()
    #
    # def rule_19(self, val):
    #     raise ValueError()




