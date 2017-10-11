import logging, boto3, botocore.exceptions, json, urllib


TABLE = "counter-upload-processor-state"
FILENAME_PREFIX = 'values_'

def lambda_handler(event, context):
  logging.getLogger().setLevel(logging.INFO)
  for record in event['Records']:
    if 'aws:sns' == record['EventSource'] and record['Sns']['Message']:
      handle_sns_event(json.loads(record['Sns']['Message']), context)
  return True


def handle_sns_event(event, context):
  for record in event['Records']:
    logging.info('looking at {}'.format(record))
    if 'aws:s3' == record['eventSource'] \
      and record['eventName'].startswith('ObjectCreated:'):
      region = record['awsRegion']
      bucket_name = record['s3']['bucket']['name']
      key_name = urllib.unquote(record['s3']['object']['key'])
      key_vsn = record['s3']['object'].get('versionId')
      logging.info('new object: s3://{}/{} (v:{})'.format(bucket_name,
                                                          key_name,
                                                          key_vsn))
      key = boto3.resource('s3', region_name=region) \
                 .Bucket(bucket_name) \
                 .Object(key_name)
      data = key.get(**{'VersionId': key_vsn} if key_vsn else {})
      process_key(region, key, data, context)


def process_key(region, key, data, context):
  filename = key.key.split('/')[-1]
  if filename.startswith(FILENAME_PREFIX):
    date = key.key.split('/')[-2]
    instance_id = filename.split('_')[1]
    logging.info('processing ({}, {})'.format(date, instance_id))
    for line in data['Body'].read().splitlines():
      counter, value = [x.strip() for x in line.split()]
      update_instance_value(region, date, instance_id, counter, value)


def update_instance_value(region, date, instance_id, counter, value):
  logging.info('updating instance counter value: {} {} {}'.format(
      instance_id, counter, value))
  tbl = boto3.resource('dynamodb', region_name=region) \
             .Table(TABLE)
  key = {'Counter': counter,
            'Date': date}
  # updating a document path in an item currently fails if the ancestor
  # attributes don't exist, and multiple SET expressions can't
  # (currently) be used to update overlapping document paths (even with
  # `if_not_exists`), so we must first create the `InstanceValues` map
  # if needed.  we use a condition expression to avoid needlessly
  # triggering an update event on the stream we'll create for this
  # table.  in a real application, we might first query the table to
  # check if these updates are actually needed (reads are cheaper than
  # writes).
  lax_update(tbl,
             Key=key,
             UpdateExpression='SET #valuemap = :empty',
             ExpressionAttributeNames={'#valuemap': 'InstanceValues'},
             ExpressionAttributeValues={':empty': {}},
             ConditionExpression='attribute_not_exists(#valuemap)')
  # we can now actually update the target path.  we only update if the
  # new value is different (in a real application, we might first query
  # and refrain from attempting the conditional write if the value is
  # unchanged):
  lax_update(tbl,
             Key=key,
             UpdateExpression='SET #valuemap.#key = :value',
             ExpressionAttributeNames={     '#key': instance_id,
                                       '#valuemap': 'InstanceValues'},
             ExpressionAttributeValues={':value': int(value)},
             ConditionExpression='NOT #valuemap.#key = :value')


def lax_update(table, **kwargs):
  try:
    return table.update_item(**kwargs)
  except botocore.exceptions.ClientError as exc:
    code = exc.response['Error']['Code']
    if 'ConditionalCheckFailedException' != code:
      raise