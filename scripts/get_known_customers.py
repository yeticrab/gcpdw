# -*- coding: utf-8 -*-
"""
Created on Tue Feb 13 14:35:20 2018

Query and standardise the installations with a postcode

@author: roger.gill
"""

import re
import pandas as pd
from google.cloud import bigquery
bigquery_client = bigquery.Client()

query = """
select
*
from
`sojourn.installation`
where postcode is not null and postcode != '' and postcode != '(null)' and postcode != '(NULL)'
"""

asn_query      = bigquery_client.query(query)
asn_it         = asn_query.result()
known_profiles = pd.DataFrame()
for row in asn_it:
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
        known_profiles = known_profiles.append(record)


known_profiles = known_profiles.reset_index(drop=True)


## match to postcode and write into big query

query = """
select
*
from
`external.uk_postcodes`
"""

asn_query      = bigquery_client.query(query)
asn_it         = asn_query.result()
postcodes      = pd.DataFrame()
for row in asn_it:
    record = pd.DataFrame({
            'postcode'   : row.get('postcode')
            ,'latitude'  : row.get('latitude')
            ,'longitude' : row.get('longitude')
            }, index = [0])
    postcodes = postcodes.append(record)


postcodes = postcodes.reset_index(drop=True)


