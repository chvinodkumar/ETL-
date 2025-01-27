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
for id,filename in zip(ids,filenames):
    url = f"{url_link}{id}"
    response = requests.get(url, headers=hedrs)
    response.raise_for_status()
    sheet = response.content
    file_name=f'filename'
    file_path = f"{filePath}{filename}"
    with open(filePath + file_name, 'wb+') as f:
          f.write(sheet)
    df = pd.read_csv(filePath + file_name)
    rdf = df.where(pd.notnull(df), None)
    rdf = rdf.dropna(how='all')
    rdf = rdf.to_csv(filePath + file_name , index=False)
