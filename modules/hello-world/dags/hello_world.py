import json
from datetime import timedelta, datetime
from requests import get

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

# Config variables
# dag_config = Variable.get("hello_world_variables", deserialize_json=True)

default_args = {
    'owner': 'nalin',
    'depends_on_past': True,    
    'start_date': datetime(2020, 12, 4),
    # 'end_date': datetime(2018, 12, 5),
    'email': ['nalinadhikariofficial@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

# Set Schedule: Run pipeline once a day. 
# Use cron to define exact time. Eg. 8:15am would be "15 08 * * *"
schedule_interval = "21 1 * * *"

# Define DAG: Set ID and assign default args and schedule interval
dag = DAG(
    'fake_rest_api', 
    default_args=default_args, 
    schedule_interval=schedule_interval
    )

def hello_world():
    print("Hello World")

def fetch_data():
    url = "https://jsonplaceholder.typicode.com/todos"
    response = get(url)
    if response.status_code == 200:
        print(response.text)
    else:
        print(response.status_code)

def bye_world():
    print("Bye World")

t1 = PythonOperator(
    task_id='print_hello_world',
    provide_context=False,
    python_callable=hello_world,
    dag=dag,
)

t2 = PythonOperator(
    task_id='fetch_data',
    provide_context=False,
    python_callable=fetch_data,
    dag=dag,
)

t3 = PythonOperator(
    task_id='print_bye_world',
    provide_context=False,
    python_callable=bye_world,
    dag=dag,
)

t1 >> [t2]
t2 >> [t3]