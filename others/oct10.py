import pandas as pd
import json
import requests
from datetime import date, datetime, timedelta
import boto3

curt_date = date.today()
pre_date = date.today() - timedelta(days=1)

http_proxy = "PITC-Zscaler-EMEA-London3PR.proxy.corporate.ge.com:80"
https_proxy = "PITC-Zscaler-EMEA-London3PR.proxy.corporate.ge.com:80"

proxyDict = {
    "http": http_proxy,
    "https": https_proxy
}

maxReturn = 200
client_id = '7eac73fc-81cf-4510-bcb7-2630d12c00b8'
client_secret = 'HT9kmDSKJjyce6PkRvZ2vKylLP3gazpV'
file_path = "/data/nifi/marketo_programs/global/data/program.txt"
file_pathProgramId = "/data/nifi/marketo_programs/global/data/programIDs.txt"

url_cred = f"https://005-SHS-767.mktorest.com/identity/oauth/token?client_id={client_id}&client_secret={client_secret}&grant_type=client_credentials"

# https://005-SHS-767.mktorest.com/identity/oauth/token?client_id=91b42b2d-e829-482e-8b77-5dabcf781b84&client_secret=GdY2ff1wfWwCspzTdEtRIVqrndIdPSjj&grant_type=client_credentials

response = requests.get(url_cred)
access_token = response.json().get("access_token")
print('access token: ' + access_token)

uqProgramIdList = set()
json_res = []
offset = 0
while offset >= 0:
    stroff = str(offset)
    url = f"https://005-SHS-767.mktorest.com/rest/asset/v1/programs.json?access_token={access_token}&maxReturn={maxReturn}&offset={stroff}&earliestUpdatedAt={pre_date}T00:00:00-05:00&latestUpdatedAt={curt_date}T00:00:00-05:00"
    #    url = f"https://005-SHS-767.mktorest.com/rest/asset/v1/programs.json?access_token={access_token}&maxReturn={maxReturn}&offset={stroff}"

    response = requests.get(url)
    # stop if out of scope
    if "No assets found for the given search criteria." in response.text:
        print(f"No more data. End offset (out of limit): {stroff}")

        break
    json_res.extend(response.json().get("result"))
    # get all programIDs
    for obj in response.json().get("result"):
        uqProgramIdList.add(obj.get("id"))
    offset = offset + maxReturn
    # just to track progress
    if offset % 1000 == 0:
        print(f"current offset: {stroff}")

textfile = open(file_path, "w", encoding="utf-8")
print(f'programs - writing to file... {file_path}')

# textfile.write(str(json_res))
textfile.write(json.dumps(json_res))

# for obj in json_res:
#    textfile.write(dict(obj) )

textfile.close()

textfileId = open(file_pathProgramId, "w", encoding="utf-8")
print(f'uq ProgramId list - writing to file... ({file_pathProgramId})')
textfileId.write(str(uqProgramIdList).replace('{', '').replace('}', '') + "\n")
textfileId.close()

df = pd.read_json('/data/nifi/marketo_programs/global/data/program.txt', orient='records')

df.to_csv("/data/nifi/marketo_programs/global/data/csv/program.csv", index=False)