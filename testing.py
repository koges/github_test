import os
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable 


def process_datetime(ti):
    dt = ti.xcom_pull(task_ids='get_datetime')

    if not dt:
        raise Exception('No datetime value')
    dt = str(dt)
    dt = dt.split()

    dt_dict = {
        'year': int(dt[-1]),
        'month': dt[1],
        'day': int(dt[2]),
        'time': dt[3],
        'day_of_week':dt[0]
    }
    print(f"dt_dict={dt_dict}")
    ti.xcom_push(key='processed_datetime', value=dt_dict)

def load_datetime(ti):
    dt_processed = ti.xcom_pull(task_ids='process_datetime', key='processed_datetime')
    if not dt_processed:
        raise Exception('No processed datetime value')
    
    df = pd.DataFrame([dt_processed], index=[0])
    csv_path = Variable.get('test_pipeline_csv_path')
    if os.path.exists(csv_path):
        df_header = False
        df_mode = 'a'
    else:
        df_header = True
        df_mode = 'w'

    df.to_csv(csv_path, index=False, mode=df_mode, header=df_header)


with DAG(
    dag_id='test',
    schedule_interval='*/5 * * * *',
    start_date=datetime(year= 2023, month=5, day=4),
    catchup = False
) as dag:
    # Lets specify a task
    t1 = BashOperator(
        task_id='get_datetime',
        bash_command='date'
    )

    t2 = PythonOperator(
        task_id='process_datetime',
        python_callable=process_datetime
    )

    t3 = PythonOperator(
        task_id='load_datetime',
        python_callable=load_datetime
    )

    t1 >> t2 >> t3
    # testing
    