import os
import json
import requests
import glob
import argparse
from datetime import datetime
import pandas as pd
import psycopg2
import psycopg2.extras as extras


parser = argparse.ArgumentParser()
parser.add_argument('--infile', nargs=1, help="JSON file to be processed", type=argparse.FileType('r'))
args = parser.parse_args()
contents = json.load(args.infile[0])

token=contents["token"]
url_link=contents["url"]
ids=contents["id"]
filenames=contents["filenames"]
filePath_name=contents["filePath"]

filePath = filePath_name
showpadfiles = glob.glob(filePath + '*.*')
for f in showpadfiles:
    os.remove(f)

hedrs = {"Authorization": f"Bearer {token}", "Accept": "text/csv"}
for id, filename in zip(ids, filenames):
    try:
        url = f"{url_link}{id}"
        response = requests.get(url, headers=hedrs)
        response.raise_for_status()
        sheet = response.content
        file_path = os.path.join(filePath, filename)
        with open(file_path, 'wb+') as f:
            f.write(sheet)
        df = pd.read_csv(file_path)
        rdf = df.where(pd.notnull(df), None)
        rdf = rdf.dropna(how='all')
        rdf.to_csv(file_path, index=False)

        print(f"file name: {filename}")

    except requests.exceptions.RequestException as e:
        print(f"Request error for ID {id}: {e}")

