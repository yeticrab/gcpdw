'''
google cloud platform credential in environment variable
pip install --upgrade google-cloud-bigquery
pip install --upgrade google-cloud-storage
'''

from google.cloud import storage
import re
from datetime import datetime

storage_client = storage.Client.from_service_account_json('C:/usr/yeticrab/datacrab-045e6e03b60b.json')
sojourn_bucket = storage_client.bucket('sojourn')

blobs = sojourn_bucket.list_blobs()
filenames = dict()

for blob in blobs:
    file = re.findall(r'/(.+)_', blob.name)
    if file[0] in filenames:
        filenames[file[0]] += 1
    else:
        filenames[file[0]]  = 1
        print(file)
    match = re.search(r'\d{4}-\d{2}-\d{2}', blob.name)
    if match :
        date = datetime.strptime(match.group(), '%Y-%m-%d').date()

