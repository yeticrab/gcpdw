# -*- coding: utf-8 -*-
'''
Author : R C Gill
Date   : 2018-02-08
TODO   : Dockerise, modularlise, environment variable to the config
Notes  : 
google cloud platform credential in environment variable or direct link to the file
pip install --upgrade google-cloud-bigquery
pip install --upgrade google-cloud-storage
'''
import re
import uuid
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery

# initiaties a data set
def create_dataset(client, dataset_id):
    dataset_ref = client.dataset(dataset_id)
    dataset     = bigquery.Dataset(dataset_ref)
    dataset     = bigquery_client.create_dataset(dataset)
    
    return dataset

def create_table_ref():
    return 1

def load_data_from_s3(s3_location, schema, config):
    job_name                     1= str(uuid.uuid4())
    job_config                   = bigquery.LoadJobConfig()
    job_config.sourceFormat      = config['sourceFormat']
    job_config.fieldDelimiter    = config['fieldDelimiter']
    job_config.skip_leading_rows = config['skip_leading_rows']
    
    job = bigquery_client.load_table_from_uri(s3_location, table_ref, job_config = job_config)
    job.result()
    
    return 1
    




bigquery_client = bigquery.Client()

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





