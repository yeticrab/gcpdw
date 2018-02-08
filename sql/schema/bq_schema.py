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
DeviceLocations['table_name'] = "device_locations"

# visits
visits = {}
visits['schema'] = (
  bigquery.SchemaField('venue_id', 'STRING')
  ,bigquery.SchemaField('installation_id', 'STRING')
  ,bigquery.SchemaField('enter', 'TIMESTAMP')
  ,bigquery.SchemaField('exit', 'TIMESTAMP')
)
visits['partitioning_type'] = "DAY"
visits['table_name'] = 'visits'

# visits
venues = {}
venues['schema'] = (
  bigquery.SchemaField('venue_id', 'STRING')
  ,bigquery.SchemaField('name', 'STRING')
  ,bigquery.SchemaField('longitude', 'FLOAT')
  ,bigquery.SchemaField('latitude', 'FLOAT')
)
venues['table_name'] = 'venues'

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
stepschallenge['table_name'] = 'stepschallenge'

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
organisations['include'] = 0
organisations['table_name'] = 'organisations'

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
)
installations['include'] = 0
installations['table_name'] = 'installations'

# packafe this all together
schema = {}
schema['organisations'] = organisations

# config for the job
config = {}
config['sourceFormat']   = 'CSV'
config['fieldDelimiter'] = ','
config['skip_leading_rows'] = 1
