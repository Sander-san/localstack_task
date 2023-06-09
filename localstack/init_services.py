import shutil
import boto3
import os


AWS_REGION = 'us-east-1'
ENDPOINT_URL = 'http://localhost:4566'
S3_KEY = 'test'
S3_SECRET = 'test'
BUCKET_NAME = 'test-bucket'
QUEUE_NAME = 'test-queue'
LAMBDA_ZIP = '../zip_func_lambda'


def get_boto3_client(service):
    try:
        lambda_client = boto3.client(
            service,
            region_name=AWS_REGION,
            endpoint_url=ENDPOINT_URL,
            aws_access_key_id=S3_KEY,
            aws_secret_access_key=S3_SECRET,
            verify=False
        )
    except Exception as e:
        raise e
    else:
        return lambda_client


def create_bucket():
    s3_client = get_boto3_client('s3')
    s3_client.create_bucket(Bucket=BUCKET_NAME)


def create_sqs():
    sqs_client = get_boto3_client('sqs')
    sqs_client.create_queue(QueueName=QUEUE_NAME)


def create_role():
    """Creates iam role for the function execution"""
    iam_client = get_boto3_client('iam')

    iam_client.create_policy(PolicyName='my-pol',
                             PolicyDocument="""{
      "Version": "2012-10-17",
      "Statement": [
        {
          "Effect": "Allow",
          "Action": [
            "logs:PutLogEvents",
            "logs:CreateLogGroup",
            "logs:CreateLogStream"
          ],
          "Resource": "arn:aws:logs:*:*:*"
        },
        {
          "Effect": "Allow",
          "Action": [
            "s3:GetObject",
            "s3:PutObject",
            "s3:CreateBucket"
          ],
          "Resource": "arn:aws:s3:::test-bucket/*"
        },
        {
          "Effect": "Allow",
          "Action": [
            "sqs:DeleteMessage",
            "sqs:GetQueueAttributes",
            "sqs:ReceiveMessage"
          ],
          "Resource": "arn:aws:sqs:us-east-1:000000000000:test-queue"
        },
        {
          "Effect": "Allow",
            "Action": [
                "dynamodb:BatchGetItem",
                "dynamodb:GetItem",
                "dynamodb:Query",
                "dynamodb:Scan",
                "dynamodb:BatchWriteItem",
                "dynamodb:PutItem",
                "dynamodb:UpdateItem"
            ],
            "Resource": "arn:aws:dynamodb:us-east-1:000000000000:table/*"
        }
      ]
    }""")

    iam_client.create_role(
        RoleName='localstack-s3-role',
        AssumeRolePolicyDocument="""{"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Principal": {"Service": "localstack.amazonaws.com"}, "Action": "sts:AssumeRole"}]}""")

    iam_client.attach_role_policy(
        RoleName='localstack-s3-role',
        PolicyArn='arn:aws:iam::000000000000:policy/my-pol')


def create_lambda_zip():
    """Generate ZIP file for localstack function."""
    try:
        shutil.make_archive(LAMBDA_ZIP, 'zip', './')
    except Exception as e:
        raise e


def create_lambda(function_name):
    """Creates a Lambda function in LocalStack and attach iam role"""
    try:
        lambda_client = get_boto3_client('lambda')
        create_lambda_zip()

        # create zip file for localstack function
        with open(LAMBDA_ZIP + '.zip', 'rb') as f:
            zipped_code = f.read()

        lambda_client.create_function(
            FunctionName=function_name,
            Runtime='python3.8',
            Role='arn:aws:iam::000000000000:role/localstack-s3-role',
            Handler=function_name + '.handler',
            Code=dict(ZipFile=zipped_code),
            Timeout=60
        )
    except Exception as e:
        raise e


def create_notification():
    """Creates notification for s3 bucket to sqs"""
    s3_client = get_boto3_client('s3')
    s3_client.put_bucket_notification_configuration(
        Bucket=BUCKET_NAME,
        NotificationConfiguration={
            "QueueConfigurations": [
                {
                    "QueueArn": "arn:aws:sqs:us-east-1:000000000000:test-queue",
                    "Events": [
                        "s3:ObjectCreated:*"
                    ],
                    "Filter": {
                        "Key": {
                            "FilterRules": [
                                {
                                    "Name": 'prefix',
                                    "Value": "data_by_month/"
                                }
                            ]
                        }
                    }
                },
                {
                    "QueueArn": "arn:aws:sqs:us-east-1:000000000000:test-queue",
                    "Events": [
                        "s3:ObjectCreated:*"
                    ],
                    "Filter": {
                        "Key": {
                            "FilterRules": [
                                {
                                    "Name": 'prefix',
                                    "Value": "metrics_by_month/"
                                }
                            ]
                        }
                    }
                },
            ]
        }, )


def create_event_mapping():
    """Creates event mapping to get messages from sqs to lambda"""
    lambda_client = get_boto3_client('lambda')
    lambda_client.create_event_source_mapping(
        EventSourceArn='arn:aws:sqs:us-east-1:000000000000:test-queue',
        FunctionName='lambda_handler',
        Enabled=True,
        BatchSize=1, )


def delete_function():
    lambda_f = get_boto3_client('lambda')
    lambda_f.delete_function(FunctionName='lambda_handler')
    os.remove(LAMBDA_ZIP + '.zip')


def init_all():
    create_sqs()
    create_role()
    create_bucket()
    create_lambda('lambda_handler')
    create_notification()
    create_event_mapping()


if __name__ == '__main__':
    init_all()
