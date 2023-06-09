import os
import boto3
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import count


folder_path = '/data/test_data/'
s3_bucket = 'helsinki-city-bikes-bucket'
raw_data_folder_bucket = 'data_by_month'
metric_data_folder_bucket = 'metrics_by_month'
s3_client = boto3.client('s3', endpoint_url='http://localhost:4566')


def check_files():
    files = os.listdir(folder_path)
    if len(files) > 0:
        print(f'Found {len(files)} files')
    else:
        raise ValueError("Empty directory. DAG stopped.")


def upload_raw_files():
    files = os.listdir(folder_path)
    for file in files:
        file_path = os.path.join(folder_path, file)
        s3_key = os.path.join(raw_data_folder_bucket, file)
        s3_client.upload_file(file_path, s3_bucket, s3_key)
        print(f"File '{file}' uploaded to S3 bucket '{s3_bucket}' with key '{s3_key}'.")


def upload_metrics():
    spark = SparkSession.builder.getOrCreate()
    files = os.listdir(folder_path)
    for file in files:
        df = spark.read.csv(f'{folder_path}{file}', header=True)
        departure_count = df.groupBy('departure_name').agg(count('departure_name').alias('count_of_departures'))
        departure_count = departure_count.withColumnRenamed('departure_name', 'station_name')
        return_count = df.groupBy('return_name').agg(count('return_name').alias('count_of_returns'))
        return_count = return_count.withColumnRenamed('return_name', 'station_name')
        combined_counts = departure_count.join(return_count, 'station_name')

        output_data = combined_counts.toPandas().to_csv()
        file_name = file.split('.')[0]
        s3_client.put_object(Body=output_data,
                             Bucket=s3_bucket,
                             Key=f'{metric_data_folder_bucket}/{file_name}-get_bike_count.csv')
        print(f"File '{file_name}-get_bike_count.csv' "
              f"uploaded to S3 bucket '{s3_bucket}' "
              f"with key '{metric_data_folder_bucket}/{file_name}-get_bike_count.csv'.")


with DAG("upload_to_s3",
         start_date=datetime(2021, 1, 1),
         schedule_interval="@daily",
         catchup=False) as dag:

    check_files = PythonOperator(
        task_id="check_files",
        python_callable=check_files
    )

    upload_raw_files = PythonOperator(
        task_id="upload_raw_files",
        python_callable=upload_raw_files
    )

    upload_metrics = PythonOperator(
        task_id="upload_metrics",
        python_callable=upload_metrics
    )

    check_files >> [upload_raw_files, upload_metrics]
