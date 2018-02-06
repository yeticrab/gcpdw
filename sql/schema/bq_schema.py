from google.cloud import bigquery
# location table schema dictionary
location = {}
location['schema'] = (
  bigquery.SchemaField('installationId', 'STRING')
  ,bigquery.SchemaField('timestamp', 'TIMESTAMP')
  ,bigquery.SchemaField('longitude', 'FLOAT')
  ,bigquery.SchemaField('latitude', 'FLOAT')
  ,bigquery.SchemaField('accuracy', 'FLOAT')
  ,bigquery.SchemaField('speed', 'FLOAT')
  ,bigquery.SchemaField('course', 'FLOAT')
)
location['partitioning_type'] = "DAY"

# visits
visits = {}
visits['schema'] = (
  bigquery.SchemaField('venueId', 'STRING')
  ,bigquery.SchemaField('installationId', 'STRING')
  ,bigquery.SchemaField('enter', 'TIMESTAMP')
  ,bigquery.SchemaField('exit', 'TIMESTAMP')
)
visits['partitioning_type'] = "DAY"

# visits
venues = {}
venues['schema'] = (
  bigquery.SchemaField('venueId', 'STRING')
  ,bigquery.SchemaField('name', 'STRING')
  ,bigquery.SchemaField('longitude', 'FLOAT')
  ,bigquery.SchemaField('latitude', 'FLOAT')
)

# stepschallenge
stepschallenge = {}
stepschallenge['schema'] = (
  bigquery.SchemaField('installationId', 'STRING')
  ,bigquery.SchemaField('steps', 'INTEGER')
  ,bigquery.SchemaField('distance', 'FLOAT')
  ,bigquery.SchemaField('duration', 'FLOAT')
  ,bigquery.SchemaField('timestamp', 'TIMESTAMP')
  ,bigquery.SchemaField('orgId', 'STRING')
)
stepschallenge['partitioning_type'] = "DAY"

# organisations
organisations = {}
organisations['schema'] = (
  bigquery.SchemaField('organisationId', 'STRING')
  ,bigquery.SchemaField('name', 'INTEGER')
  ,bigquery.SchemaField('distance', 'FLOAT')
  ,bigquery.SchemaField('duration', 'FLOAT')
  ,bigquery.SchemaField('timestamp', 'TIMESTAMP')
  ,bigquery.SchemaField('orgId', 'STRING')
)
organisations['include'] = 0

# organisations
organisations = {}
organisations['schema'] = (
  bigquery.SchemaField('organisationId', 'STRING')
  ,bigquery.SchemaField('name', 'INTEGER')
  ,bigquery.SchemaField('distance', 'FLOAT')
  ,bigquery.SchemaField('duration', 'FLOAT')
  ,bigquery.SchemaField('timestamp', 'TIMESTAMP')
  ,bigquery.SchemaField('orgId', 'STRING')
)
organisations['include'] = 0

# organisations
installations = {}
installations['schema'] = (
  bigquery.SchemaField('id', 'STRING')
  ,bigquery.SchemaField('idfa', 'STRING')
  ,bigquery.SchemaField('gender', 'STRING')
  ,bigquery.SchemaField('postcode', 'STRING')
  ,bigquery.SchemaField('dob', 'STRING')
  ,bigquery.SchemaField('join_date', 'TIMESTAMP')
  ,bigquery.SchemaField('username', 'STRING')
  ,bigquery.SchemaField('installationId', 'STRING')
)
installations['include'] = 0


# config for the job
config = {}
config['sourceFormat']   = 'CSV'
config['fieldDelimiter'] = ','
config['skip_leading_rows'] = 1
