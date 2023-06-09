import json
import pandas as pd
import csv
import io
from init_services import get_boto3_client, BUCKET_NAME


def get_prefix(key):
    prefix = str(key).split('/')
    return prefix[0]


def create_dynamo_table(table_name, attribute_name='id', attribute_type='N'):
    dynamodb = get_boto3_client('dynamodb')
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': attribute_name,
                'KeyType': 'HASH'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': attribute_name,
                'AttributeType': attribute_type
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1
        }
    )
    return dynamodb


def raw_data_to_dynamo(key):
    prefix = get_prefix(key)

    dynamodb = create_dynamo_table(prefix)

    s3 = get_boto3_client('s3')
    item = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    csv_data = item['Body'].read().decode('utf-8')
    csv_stream = io.StringIO(csv_data)
    csv_reader = csv.DictReader(csv_stream)

    for row in csv_reader:
        item = {
            'id': {'N': str(csv_reader.line_num - 1)},
            'departure': {'S': row['departure']},
            'return': {'S': row['return']},
            'departure_id': {'S': row['departure_id']},
            'departure_name': {'S': row['departure_name']},
            'return_id': {'S': row['return_id']},
            'return_name': {'S': row['return_name']},
            'distance': {'N': row['distance (m)']},
            'duration': {'N': row['duration (sec.)']},
            'avg_speed': {'N': row['avg_speed (km/h)']},
            'departure_latitude': {'N': row['departure_latitude']},
            'departure_longitude': {'N': row['departure_longitude']},
            'return_latitude': {'N': row['return_latitude']},
            'return_longitude': {'N': row['return_longitude']},
            'air_temperature': {'N': row['Air temperature (degC)']}
        }

        dynamodb.put_item(
            TableName=prefix,
            Item=item
        )


def avg_metric_to_dynamo(key):
    s3 = get_boto3_client('s3')
    item = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    df = pd.read_csv(item['Body'])

    df['departure'] = pd.to_datetime(df['departure'])
    df['return'] = pd.to_datetime(df['return'])
    df['day'] = df['departure'].dt.date
    avg_distance_day = df.groupby('day')['distance (m)'].mean().reset_index().round(2)
    avg_duration_day = df.groupby('day')['duration (sec.)'].mean().reset_index().round(2)
    avg_speed_day = df.groupby('day')['avg_speed (km/h)'].mean().reset_index().round(2)
    avg_temperature_day = df.groupby('day')['Air temperature (degC)'].mean().reset_index().round(2)
    avg_day = pd.merge(avg_distance_day, avg_duration_day, on='day')
    avg_day = pd.merge(avg_day, avg_speed_day, on='day')
    avg_day = pd.merge(avg_day, avg_temperature_day, on='day')

    table_name = 'avg_metrics_by_day'
    dynamodb = create_dynamo_table(table_name)

    output_data = avg_day.to_csv()
    csv_stream = io.StringIO(output_data)
    csv_reader = csv.DictReader(csv_stream)

    for row in csv_reader:
        item = {
            'id': {'N': (int(row['']) + 1)},
            'day': {'S': row['day']},
            'distance (m)': {'N': row['distance (m)']},
            'duration (sec.)': {'N': row['duration (sec.)']},
            'avg_speed (km/h)': {'N': row['avg_speed (km/h)']},
            'Air temperature (degC)': {'N': row['Air temperature (degC)']},
        }

        dynamodb.put_item(
            TableName=table_name,
            Item=item
        )


def metric_to_dynamo(key):
    prefix = get_prefix(key)

    dynamodb = create_dynamo_table(prefix)

    s3 = get_boto3_client('s3')
    item = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    csv_data = item['Body'].read().decode('utf-8')
    csv_stream = io.StringIO(csv_data)
    csv_reader = csv.DictReader(csv_stream)

    for row in csv_reader:
        item = {
            'id': {'N': (int(row['']) + 1)},
            'station_name': {'S': row['station_name']},
            'count_of_departures': {'N': row['count_of_departures']},
            'count_of_returns': {'N': row['count_of_returns']},
        }

        dynamodb.put_item(
            TableName=prefix,
            Item=item
        )


def handler(event, context):
    body = event['Records'][0]['body']
    body_dict = json.loads(body)
    if 'Event' in body_dict and body_dict['Event'] == 's3:TestEvent':
        print(f"Test Event - {body_dict['Event']}")
    else:
        key = body_dict['Records'][0]['s3']['object']['key']
        if 'data_by_month' in key:
            print(f'Raw data - {key}')
            raw_data_to_dynamo(key)
            avg_metric_to_dynamo(key)
        else:
            print(f'Metric data - {key}')
            metric_to_dynamo(key)

