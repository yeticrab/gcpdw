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
from google.cloud import storage
from google.cloud import bigquery


def if_tbl_exists(client, table_ref):
    from google.cloud.exceptions import NotFound
    try:
        client.get_table(table_ref)
        return True
    except NotFound:
        return False
    
# initiaties a data set
def create_dataset(client, dataset_id):
    dataset_ref = client.dataset(dataset_id)
    dataset     = bigquery.Dataset(dataset_ref)
    dataset     = bigquery_client.create_dataset(dataset)
    
    return dataset

def create_table_ref(client, blob, schema):
    file_type   = re.findall(r'/(.+)_', blob.name)[0]
    file_schema = schema[file_type]
    date        = ''
    if 'partitioning_type' in file_schema:
        match = re.search(r'\d{4}-\d{2}-\d{2}', blob.name)
        date  = '_' + match.group()
    
    data_set  = client.dataset(schema[file_type]['table_name'])
    table_ref = data_set.table(schema[file_type]['table_name'] + date)
    table     = bigquery.Table(table_ref)
    table.schema = file_schema['schema']
    if 'partitioning_type' in file_schema:
        table.partitioning_type = file_schema['partitioning_type']
    
    if if_tbl_exists(bigquery_client, table_ref):
        return table_ref
    
    client.create_table(table)   
    
    return table_ref

def load_data_from_s3(client, blob, schema, config, overwrite = False):
    
    job_config                   = bigquery.LoadJobConfig()
    job_config.sourceFormat      = config['sourceFormat']
    job_config.fieldDelimiter    = config['fieldDelimiter']
    job_config.skip_leading_rows = config['skip_leading_rows']
    if overwrite:
        job_config.write_disposition = 'WRITE_TRUNCATE'
    
    s3_location      = "gs://{0}/{1}".format(blob.bucket.name, blob.name)
    table_ref = create_table_ref(client, blob, schema)
    job       = client.load_table_from_uri(s3_location, table_ref, job_config = job_config)
    
    return job


def create_base_datasets(client, blobs, schema):
    
    filenames = {}
    for blob in blobs:
        file = re.findall(r'/(.+)_', blob.name)
        if file[0] in filenames:
            filenames[file[0]] += 1
        else:
            filenames[file[0]]  = 1
    
    for key in filenames:
        if key not in schema:
            continue
        if schema[key]['include'] == False:
            continue
        # The name for the new dataset - check if required
        dataset_id  = schema[key]['table_name']
        # Prepares a reference to the new dataset
        dataset_ref = client.dataset(dataset_id)
        dataset     = bigquery.Dataset(dataset_ref)
        
        # Creates the new dataset
        dataset     = client.create_dataset(dataset)
    

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

def overwrite_dimensional_data(table_id, client, bucket, schema, config):
    blobs    = bucket.list_blobs()
    max_date = get_latest_s3_blob(table_id, blobs)
    max_blob = bucket.get_blob("test/{0}_{1}.csv".format(table_id, max_date))
    load_job = load_data_from_s3(bigquery_client, max_blob, schema, config, overwrite = True)
    load_job.result()
    print("loaded {0} lines of data".format(load_job.output_))
    return 1

def load_historical_data(table_id, client, bucket, schema, config, overwrite = False):
    blobs    = bucket.list_blobs()
        file = re.findall(r'/(.+)_', blob.name)

########################################
########################################
########################################

bigquery_client = bigquery.Client()
storage_client  = storage.Client.from_service_account_json('C:/usr/yeticrab/datacrab-045e6e03b60b.json')
sojourn_bucket  = storage_client.bucket('sojourn')

overwrite_dimensional_data('venues', bigquery_client, sojourn_bucket, schema, config)
overwrite_dimensional_data('installations', bigquery_client, sojourn_bucket, schema, config)

sojourn_blobs   = sojourn_bucket.list_blobs()

########################################
########################################
########################################
