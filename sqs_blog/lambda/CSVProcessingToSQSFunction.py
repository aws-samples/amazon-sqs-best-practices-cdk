import json
import boto3
import csv
import os

def lambda_handler(event, context):

    print(event)
    # Retrieve the S3 bucket and object key from the SQS message
    
    record = event['Records'][0]

    source_bucket = record['s3']['bucket']['name']
    key = record['s3']['object']['key']

    # Read the CSV file from S3
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=source_bucket, Key=key)
    csv_content = response['Body'].read().decode('utf-8-sig')

    # Initialize the SQS client
    sqs_client = boto3.client('sqs')
    queue_url = os.environ['SQS_QUEUE_URL']

    # Parse the CSV records and send them to SQS as batch messages
    csv_reader = csv.DictReader(csv_content.splitlines())
    message_batch = []
    for row in csv_reader:
        # Convert the row to JSON
        json_message = json.dumps(row)

        # Add the message to the batch
        message_batch.append({
            'Id': str(len(message_batch) + 1),
            'MessageBody': json_message
        })

        # Send the batch of messages when it reaches the maximum batch size (10 messages)
        if len(message_batch) == 10:
            sqs_client.send_message_batch(
                QueueUrl=queue_url,
                Entries=message_batch
            )
            message_batch = []
            print('Sent messages in batch')

    # Send any remaining messages in the batch
    if message_batch:
        sqs_client.send_message_batch(
            QueueUrl=queue_url,
            Entries=message_batch
        )
