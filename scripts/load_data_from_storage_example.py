# -*- coding: utf-8 -*-
"""
Created on Wed Feb 14 12:06:05 2018

Quick hacky script to load data from storage

@author: roger.gill
"""


from google.cloud import storage
from google.cloud import bigquery

storage_bucket    = 'filder'
filename          = 'known_profiles.csv'
cred_json         = 'C:/usr/yeticrab/datacrab-045e6e03b60b.json'
destination_table = 'fidler'
destination_table_name = 'known_profiles'

storage_client  = storage.Client.from_service_account_json(cred_json)
bucket          = storage_client.bucket(storage_bucket)
blob            = bucket.get_blob(filename)
s3_location     = "gs://{0}/{1}".format(blob.bucket.name, blob.name)


bigquery_client = bigquery.Client()

postcode_config = (
  bigquery.SchemaField('id', 'STRING')
  ,bigquery.SchemaField('idfa', 'STRING')  
  ,bigquery.SchemaField('installation_id', 'STRING')  
  ,bigquery.SchemaField('postcode', 'STRING') 
  ,bigquery.SchemaField('pid', 'FLOAT')  
  ,bigquery.SchemaField('latitude', 'FLOAT')
  ,bigquery.SchemaField('longitude', 'FLOAT')
)

job_config                   = bigquery.LoadJobConfig()
job_config.sourceFormat      = 'CSV'
job_config.fieldDelimiter    = ','
job_config.skip_leading_rows = 1


data_set  = bigquery_client.dataset(destination_table)
table_ref = data_set.table(destination_table_name)
table     = bigquery.Table(table_ref)
table.schema = postcode_config
bigquery_client.create_table(table)   
    
job       = bigquery_client.load_table_from_uri(s3_location, table_ref, job_config = job_config)
job.result()
