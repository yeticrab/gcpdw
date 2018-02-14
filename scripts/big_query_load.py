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
from datetime     import datetime
from google.cloud import storage
from google.cloud import bigquery

########################################
########################################
########################################

# checks if table exisits
def if_tbl_exists(client, table_ref):
    from google.cloud.exceptions import NotFound
    try:
        client.get_table(table_ref)
        return True
    except NotFound:
        return False

# initiaties a data set - currently only the sojourn data
def create_dataset(client, dataset_id):
    dataset_ref = client.dataset(dataset_id)
    dataset     = bigquery.Dataset(dataset_ref)
    dataset     = bigquery_client.create_dataset(dataset)
    
    return dataset

def create_table_ref(client, dataset_id, blob, schema):
    file_type   = re.findall(r'/(.+)_', blob.name)[0]
    file_schema = schema[file_type]
    date        = ''
    if 'partitioning_type' in file_schema:
        match = re.search(r'\d{4}-\d{2}-\d{2}', blob.name)
        date  = match.group()
        date  = datetime.strptime(date,'%Y-%m-%d')
        date  = date.strftime('%Y%m%d')
        date  = '_' + date
    
    data_set  = client.dataset(dataset_id)
    table_ref = data_set.table(schema[file_type]['table_name'] + date)
    table     = bigquery.Table(table_ref)
    table.schema = file_schema['schema']
    if 'partitioning_type' in file_schema:
        table.partitioning_type = file_schema['partitioning_type']
    
    if if_tbl_exists(bigquery_client, table_ref):
        return table_ref
    
    client.create_table(table)   
    
    return table_ref

def load_data_from_s3(client, dataset_id, blob, schema, config, overwrite = False):
    
    job_config                   = bigquery.LoadJobConfig()
    job_config.sourceFormat      = config['sourceFormat']
    job_config.fieldDelimiter    = config['fieldDelimiter']
    job_config.skip_leading_rows = config['skip_leading_rows']
    job_config.max_bad_records   = config['max_bad_records']
    if overwrite:
        job_config.write_disposition = 'WRITE_TRUNCATE'
    
    s3_location      = "gs://{0}/{1}".format(blob.bucket.name, blob.name)
    table_ref = create_table_ref(client, dataset_id, blob, schema)
    if if_tbl_exists(client, table_ref) & (overwrite == False):
        return 1
    job       = client.load_table_from_uri(s3_location, table_ref, job_config = job_config)
    job.result()
    print("loaded {0} lines of data".format(job.output_rows))
    return 1


def overwrite_dimensional_data(client, dataset_id, table_id, bucket, schema, config):
    blobs    = bucket.list_blobs()
    max_date = get_latest_s3_blob(table_id, blobs)
    max_blob = bucket.get_blob("test/{0}_{1}.csv".format(table_id, max_date))
    load_job = load_data_from_s3(bigquery_client, dataset_id, max_blob, schema, config, overwrite = True)
    load_job.result()
    print("loaded {0} lines of data".format(load_job.output_rows))
    return 1

def load_historical_data(client, dataset_id, table_id, bucket, schema, config, overwrite = False):
    blobs    = bucket.list_blobs()
    for blob in blobs:
        file = re.findall(r'/(.+)_', blob.name)[0]
        if file != table_id:
            continue
        print("loading {0} into big query".format(blob.name))
        load_job = load_data_from_s3(client, dataset_id, blob, schema, config, overwrite)


########################################
########################################
########################################
        
def get_latest_s3_blob(table_id, blobs):
    
    max_date  = '2000-00-00'
    
    for blob in blobs:
        file = re.findall(r'/(.+)_', blob.name)[0]
        if file != table_id:
            continue
        
        match = re.search(r'\d{4}-\d{2}-\d{2}', blob.name)
        if match :
            date = match.group()
            if date > max_date :
                max_date = date
    
    return max_date

########################################
########################################
########################################

bigquery_client = bigquery.Client()
storage_client  = storage.Client.from_service_account_json('C:/usr/yeticrab/datacrab-045e6e03b60b.json')
sojourn_bucket  = storage_client.bucket('sojourn')


overwrite_dimensional_data(bigquery_client, 'sojourn', 'venues', sojourn_bucket, schema, config)
overwrite_dimensional_data(bigquery_client, 'sojourn', 'installations', sojourn_bucket, schema, config)
load_historical_data(bigquery_client, 'sojourn', 'stepschallenge', sojourn_bucket, schema, config)
load_historical_data(bigquery_client, 'sojourn', 'DeviceLocations',  sojourn_bucket, schema, config, overwrite = True)
load_historical_data(bigquery_client, 'sojourn', 'visits', sojourn_bucket, schema, config)

## overwrite function
## skip rows with errors - write to log


########################################
########################################
########################################

# list tables
# find row count 
# delete
dataset_ref = bigquery_client.dataset('sojourn')
tables = list(bigquery_client.list_dataset_tables(dataset_ref))

for table in tables:
    print(1)

########################################
########################################
########################################

2017-05-19.cs