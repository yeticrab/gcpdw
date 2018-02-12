from google.cloud import bigquery
# location table schema dictionary
DeviceLocations = {}
DeviceLocations['schema'] = (
  bigquery.SchemaField('id', 'STRING')
  ,bigquery.SchemaField('timestamp', 'TIMESTAMP')
  ,bigquery.SchemaField('longitude', 'FLOAT')
  ,bigquery.SchemaField('latitude', 'FLOAT')
  ,bigquery.SchemaField('accuracy', 'FLOAT')
  ,bigquery.SchemaField('speed', 'FLOAT')
  ,bigquery.SchemaField('course', 'FLOAT')
)
DeviceLocations['partitioning_type'] = "DAY"
DeviceLocations['table_name']        = "device_location"
DeviceLocations['include']           = True

# visits
visits = {}
visits['schema'] = (
  bigquery.SchemaField('venue_id', 'STRING')
  ,bigquery.SchemaField('installation_id', 'STRING')
  ,bigquery.SchemaField('enter', 'TIMESTAMP')
  ,bigquery.SchemaField('exit', 'TIMESTAMP')
)
visits['partitioning_type'] = "DAY"
visits['table_name']        = 'visit'
visits['include']           = True

# venues
venues = {}
venues['schema'] = (
  bigquery.SchemaField('venue_id', 'STRING')
  ,bigquery.SchemaField('name', 'STRING')
  ,bigquery.SchemaField('longitude', 'FLOAT')
  ,bigquery.SchemaField('latitude', 'FLOAT')
)
venues['table_name'] = 'venue'
venues['include']    = True

# stepschallenge
stepschallenge = {}
stepschallenge['schema'] = (
  bigquery.SchemaField('installation_id', 'STRING')
  ,bigquery.SchemaField('steps', 'INTEGER')
  ,bigquery.SchemaField('distance', 'FLOAT')
  ,bigquery.SchemaField('duration', 'FLOAT')
  ,bigquery.SchemaField('timestamp', 'TIMESTAMP')
  ,bigquery.SchemaField('org_id', 'STRING')
)
stepschallenge['partitioning_type'] = "DAY"
stepschallenge['table_name']        = 'step_challenge'
stepschallenge['include']           = True

# organisations
organisations = {}
organisations['schema'] = (
  bigquery.SchemaField('organisation_id', 'STRING')
  ,bigquery.SchemaField('name', 'INTEGER')
  ,bigquery.SchemaField('distance', 'FLOAT')
  ,bigquery.SchemaField('duration', 'FLOAT')
  ,bigquery.SchemaField('timestamp', 'TIMESTAMP')
  ,bigquery.SchemaField('org_id', 'STRING')
)

organisations['table_name'] = 'organisation'
organisations['include']    = False

# installations
installations = {}
installations['schema'] = (
  bigquery.SchemaField('installation_id', 'STRING')
  ,bigquery.SchemaField('idfa', 'STRING')
  ,bigquery.SchemaField('gender', 'STRING')
  ,bigquery.SchemaField('postcode', 'STRING')
  ,bigquery.SchemaField('dob', 'STRING')
  ,bigquery.SchemaField('join_date', 'TIMESTAMP')
  ,bigquery.SchemaField('username', 'STRING')
  ,bigquery.SchemaField('id', 'STRING')
  ,bigquery.SchemaField('nickname', 'STRING')
  ,bigquery.SchemaField('org_username', 'STRING')
  ,bigquery.SchemaField('org_name', 'STRING')
  ,bigquery.SchemaField('org_id', 'STRING')
)

installations['table_name'] = 'installation'
installations['include']    = True

# ActivityData

activitydata = {}
activitydata['include']    = False

# package this all together
schema = {}
schema['DeviceLocations'] = DeviceLocations
schema['visits']          = visits
schema['venues']          = venues
schema['stepschallenge']  = stepschallenge
schema['organisations']   = organisations
schema['installations']   = installations
schema['ActivityData']    = activitydata

# config for the job
config = {}
config['sourceFormat']      = 'CSV'
config['fieldDelimiter']    = ','
config['skip_leading_rows'] = 1
