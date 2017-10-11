import logging, boto3, botocore.exceptions, json, urllib

USER_TABLE = "User"
EVENT_TABLE = "Event"
REGION = "us-east-1"
STATE = "CLEAR"
MSG = """Thank you for using our service to get relief. 
Please longpress when you need relief again"""
TYPE = "SINGLE"

# @Author - Pratik P
# handles the following
# {
#  "serialNumber": "ABCDEFG12345",
#  "clickType": "SINGLE",
#  "batteryVoltage": "2000 mV"
# }

def lambda_handler(event, context):
  logging.getLogger().setLevel(logging.INFO)
  print (event)
  print (context)
  clickType = event['clickType']
  if (clickType != str(TYPE)):
    logging.info('Click Type: {} is not supported'.format(clickType))
    return False

  dsn = event['serialNumber']
  process_key(event, dsn, context)
  return True

# Handles the following
# if key exists in USER-TABLE
    #process key
# if key *doesn't* exist in EVENT-TABlE
    #add row
# if key exist in EVENT-TABlE
    #update row
def process_key(event, dsn, context):
  logging.info('Processing dsn id: {}'.format(dsn))
  userTable = boto3.resource('dynamodb', region_name=REGION) \
             .Table(USER_TABLE)
  
  response = userTable.get_item(Key={'DeviceId':str(dsn)})
  print('response: ')
  print(response)
  
  if 'Item' in response:
    logging.info('dsn id: {} exists in the User table. Adding/Updating Events table with Single click'.format(dsn))
    eventTable = boto3.resource('dynamodb', region_name=REGION).Table(EVENT_TABLE)
    resp = eventTable.put_item(Item={'DeviceId':str(dsn), 'State': str(STATE)})
    print(resp)
    handle_sns_event(event, context)
  else :
    logging.info('dsn id: {} does *not* exist in the User table'.format(dsn))
    
def handle_sns_event(event, context):
    client = boto3.client('sns')
    snsResponse = client.publish(
        TargetArn='arn:aws:sns:us-east-1:597248753215:ConfirmLongPress',
        Message= str(MSG),
        MessageStructure='raw')