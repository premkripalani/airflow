# Import the necessary libraries
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.http import HttpSensor  
from airflow.models import Variable  
from datetime import datetime, timedelta
import json
from airflow.operators.s3 import S3CreateBucketOperator  
import requests
import pandas as pd


#define the default arguments
default_args = {
    'owner': 'pk',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

#set openweather api endpoing and parameters
api_endpoint = 'http://api.openweathermap.org/data/2.5/forcast'
api_endpoint = Variable.get('api_endpoint')
api_params = {
    "q": "Melbourne,Australia",
    "appid": ValueError("key")
}

#define the function to get api data

def extract_openweather_data(**kwargs):
    print(f"Extracting data from {api_endpoint}")
    ti = kwargs['ti']
    response = requests.get(api_endpoint, params=api_params)
    data = response.json()
    print(data)
    df = pd.json_normalize(data['list'])
    print(df)
    ti.xcom_push(key = 'final_data', value = df.to_csv(index=False))

#define the dag
dag = DAG (
    dag_id='openweather_api_dag',
    default_args=default_args,
    description='A simple api data load for openweather',
    start_date=datetime(2024, 5, 1),
    schedule_interval='@once',
    catchup=False
)

#define the task
extract_api_data = PythonOperator(
    task_id='extract_api_data',
    python_callable=extract_openweather_data,
    op_kwargs={'api_endpoint': api_endpoint},
    provide_context=True,
    dag=dag
)

upload_to_s3 = S3CreateBucketOperator(
    task_id='upload_to_s3',
    aws_conn_id='aws_default',
    bucket_name='airflow_output_s3',
    key = 'raw/weather_api_data.csv',
    data="{{ti.xcom_pull(key='final_data')}}",
    dag=dag
)

#set task depandancy
extract_api_data >> upload_to_s3
