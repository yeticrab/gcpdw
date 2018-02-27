# -*- coding: utf-8 -*-
"""
Created on Tue Feb 13 14:35:20 2018

Query and standardise the installations with a postcode

@author: roger.gill
"""

import re
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
bigquery_client = bigquery.Client()

query = """
select
*
from
`sojourn.installation`
where postcode is not null and postcode != '' and postcode != '(null)' and postcode != '(NULL)'
"""

known_profiles = pd.read_gbq(query, 'datacrab-186315', dialect = 'standard')
known_profiles = known_profiles.reset_index(drop=True)

profiles = pd.DataFrame()

for index, row in known_profiles.iterrows():
    pcode = row.get('postcode')
    pcode = re.sub('[^A-Za-z0-9]+', '', pcode)
    pcode = pcode.upper()
    pmat  = re.findall(r'\b[A-Z]{1,2}[0-9][A-Z0-9]?[0-9][ABD-HJLNP-UW-Z]{2}\b', pcode)
    if len(pmat) == 1:
        record = pd.DataFrame({
                'installation_id' : row.get('installation_id')
                ,'id' : row.get('id')
                ,'postcode' : pmat[0]
                ,'idfa' : row.get('idfa')
                }, index = [0])
        profiles = profiles.append(record)


profiles = profiles.reset_index(drop=True)


## match to postcode and write into big query

from google.cloud import storage
import pandas as pd

storage_bucket    = 'sworn'
filename          = 'ukpostcodes.csv'
cred_json         = ''

storage_client  = storage.Client.from_service_account_json(cred_json)
bucket          = storage_client.bucket(storage_bucket)
blob            = bucket.get_blob(filename)

blob.download_to_filename('c:\\usr\\test.csv')
postcodes       = pd.read_csv('c:\\usr\\test.csv')
postcodes.postcode = postcodes.postcode.replace(' ','', regex = True)
profiles  = pd.merge(known_profiles, postcodes, 'left', on = 'postcode')
profiles  = profiles[~profiles.latitude.isin(['NaN'])]
profiles.to_csv('c:\\usr\\test.csv', index = False)

## now we load to storage

storage_client  = storage.Client.from_service_account_json('C:/usr/yeticrab/datacrab-045e6e03b60b.json')
bucket          = storage_client.bucket(storage_bucket)
blob            = bucket.blob('known_profiles.csv')
blob.upload_from_filename('c:\\usr\\test.csv')

## and into bigquery

