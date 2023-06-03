import json
import os
import boto3
import uuid

def lambda_handler(event, context):
    # Retrieve the messages from the SQS event
    messages = event['Records']

    print(messages)
    
    # Initialize the DynamoDB client
    dynamodb_client = boto3.client('dynamodb')
    table_name = os.environ['DYNAMODB_TABLE_NAME']

    # Process each message in the batch
    for message in messages:
        # Retrieve the JSON message body
        message_body = json.loads(message['body'])

        # Generate UUID for the record
        record_id = str(uuid.uuid4())

        # Prepare the item to be inserted into DynamoDB
        item = {
            'id': {'S': record_id},
            'product_id': {'S': message_body['product_id']},
            'location': {'S': message_body['location']},
            'quantity': {'N': str(message_body['quantity'])},
            'update_date': {'S': message_body['update_date']}
        }

        # Insert the item into DynamoDB
        dynamodb_client.put_item(TableName=table_name, Item=item)
