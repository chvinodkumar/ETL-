import json
import pandas as pd

json_file = '/home/lh358774/json.json'

with open(json_file, 'r') as f:
    data = json.load(f)
csv_files = data['csv_files']

csv_column_dtypes = {}

for csv_file in csv_files:
    try:
        df = pd.read_csv(csv_file)
        column_dtypes = df.dtypes.to_dict()
        csv_column_dtypes[csv_file] = column_dtypes

    except FileNotFoundError:
        print(f"File {csv_file} not found.")
    except Exception as e:
        print(f"Error reading {csv_file}: {str(e)}")

for csv_file, dtypes in csv_column_dtypes.items():
    print(f"\nData types in {csv_file}:")
    for column, dtype in dtypes.items():
        print(f"{column}: {dtype}")


# output_json = 'csv_column_dtypes.json'
# with open(output_json, 'w') as f:
#     json.dump(csv_column_dtypes, f, indent=4)
#
#
# import os
# file= "config.json"
# print(os.path.abspath(file))