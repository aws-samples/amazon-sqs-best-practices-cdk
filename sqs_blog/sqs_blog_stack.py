#CSV > S3 > Lambda > SQS > Lambda > DynamoDb

from aws_cdk import (
    CfnOutput,
    Duration,
    Stack,
    aws_sqs as sqs,
    aws_s3 as s3,
    aws_s3_notifications as s3_notifications,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_sns as sns,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as cloudwatch_actions,
    aws_dynamodb as dynamodb,
    Tags
)
from constructs import Construct
from aws_cdk.aws_lambda import Function, Tracing

class SqsBlogStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create a role for the Lambda function
        role = iam.Role(
            self, "InventoryFunctionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            role_name="InventoryFunctionRole",
            description="Role for Lambda functions"
        )

        Tags.of(role).add("department", "inventory")

        # Allow the Lambda function to write to CloudWatch Logs
        role.add_to_policy(iam.PolicyStatement(
            actions=["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
            resources=["arn:aws:logs:*:*:*"]
        ))

        # Create the Dead Letter Queue (DLQ)
        dlq = sqs.Queue(self, 'InventoryUpdatesDlq',
            visibility_timeout=Duration.seconds(300)
        )

        Tags.of(dlq).add("department", "inventory")

        # Create the SQS queue with DLQ setting
        queue = sqs.Queue(
            self, "InventoryUpdatesQueue",
            visibility_timeout=Duration.seconds(300),
            #encryption=sqs.QueueEncryption.KMS_MANAGED,
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=5,  # Number of retries before sending the message to the DLQ
                queue=dlq
            )
        )

        Tags.of(queue).add("department", "inventory")

        # Allow the Lambda function to receive messages from the SQS queue
        role.add_to_policy(iam.PolicyStatement(
            actions=["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"],
            resources=[queue.queue_arn]
        ))

        # Create the DynamoDB table
        table = dynamodb.Table(self, 'InventoryUpdates',
            partition_key=dynamodb.Attribute(name='id', type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST
        )

        Tags.of(table).add("department", "inventory")

        #create input S3 bucket
        bucket = s3.Bucket(self, "InventoryUpdatesBucket")
        bucket_arn = bucket.bucket_arn

        Tags.of(bucket).add("department", "inventory")

        # Create pre-processing Lambda function
        csv_processing_to_sqs_function  = _lambda.Function(self, 'CSVProcessingToSQSFunction',
                                   runtime=_lambda.Runtime.PYTHON_3_8,
                                   code=_lambda.Code.from_asset('sqs_blog/lambda'),
                                   handler='CSVProcessingToSQSFunction.lambda_handler',
                                   role=role,
                                   tracing=Tracing.ACTIVE
                                   )
        csv_processing_to_sqs_function .add_environment('SQS_QUEUE_URL', queue.queue_url)

        # Add tags to the Lambda function
        Tags.of(csv_processing_to_sqs_function ).add("department", "inventory")

        # Grant the Lambda function read permissions to the S3 bucket
        bucket.grant_read(csv_processing_to_sqs_function )

        # Configure the bucket notification to invoke the Lambda function for all object creations
        notification = s3_notifications.LambdaDestination(csv_processing_to_sqs_function )
        bucket.add_event_notification(s3.EventType.OBJECT_CREATED, notification)


        # Create an SQS queue policy to allow source queue to send messages to the DLQ
        policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["sqs:SendMessage"],
            resources=[dlq.queue_arn],
            conditions={
                "ArnEquals": {
                    "aws:SourceArn": queue.queue_arn
                }
            }
        )
        queue.queue_policy = iam.PolicyDocument(statements=[policy])

        # Create an SNS topic for alarms
        topic = sns.Topic(self, 'InventoryUpdatesTopic')

        Tags.of(topic).add("department", "inventory")

        # Create a CloudWatch alarm for ApproximateAgeOfOldestMessage metric
        alarm = cloudwatch.Alarm(self, 'OldInventoryUpdatesAlarm',
            alarm_name='OldInventoryUpdatesAlarm',
            metric=queue.metric_approximate_age_of_oldest_message(),
            threshold=600,  # Specify your desired threshold value in seconds
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD
        )
        alarm.add_alarm_action(cloudwatch_actions.SnsAction(topic))

        Tags.of(alarm).add("department", "inventory")

        # Define the queue policy to allow messages from the Lambda function's role only
        policy = iam.PolicyStatement(
            actions=['sqs:SendMessage'],
            effect=iam.Effect.ALLOW,
            principals=[iam.ArnPrincipal(role.role_arn)],
            resources=[queue.queue_arn]
        )

        queue.add_to_resource_policy(policy)


        # Create a post-processing Lambda function with the specified role
        sqs_to_dynamodb_function  = _lambda.Function(
            self, "SQSToDynamoDBFunction",
            runtime=_lambda.Runtime.PYTHON_3_8,
            code=_lambda.Code.from_asset('sqs_blog/lambda'),
                                   handler='SQSToDynamoDBFunction.lambda_handler',
                                    role=role,
                                    tracing=Tracing.ACTIVE  # Enable active tracing with X-Ray
        )

        # Add tags to the Lambda function
        Tags.of(sqs_to_dynamodb_function ).add("department", "inventory")

        # Add DynamoDB permissions to the Lambda function
        table.grant_read_write_data(sqs_to_dynamodb_function)


        # Add the SQS queue as a trigger to the Lambda function
        sqs_to_dynamodb_function .add_event_source_mapping(
            "MyQueueTrigger",
            event_source_arn=queue.queue_arn,
            batch_size=10
        )

        sqs_to_dynamodb_function.add_environment('DYNAMODB_TABLE_NAME', table.table_name)

        Tags.of(sqs_to_dynamodb_function).add("department", "inventory")

        #Output
        CfnOutput(self, "S3 Bucket Name", value=bucket.bucket_name)
