# '''
# =================================================
# Milestone 3

# Nama  : Putri joeliya
# Batch : CODA-020-RMT

# Program ini dibuat untuk melakukan automatisasi DAG di arflow mengguanakan ETL pipeline yang sebelmnya sudah di kerjakan.
# dengan data set yang di gunakan yaitu IBM HR analytics attrition dataset
# =================================================
# '''

import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'zoelya',
    'start_date': dt.datetime(2024, 11, 1),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

# Cron expression '10,20,30 9 * * 6': Setiap Sabtu jam 09:10 AM, 09:20 AM, dan 09:30 AM
with DAG(
    'P2M3_putri-joeliya_DAG',
    default_args=default_args,
    schedule_interval='10,20,30 9 * * 6',
    catchup=False,
) as dag:

    python_extract = BashOperator(
        task_id='python_extract', 
        bash_command='sudo -u airflow python /opt/airflow/scripts/Extract_m1.py'
    )
    
    python_transform = BashOperator(
        task_id='python_transform', 
        bash_command='sudo -u airflow python /opt/airflow/scripts/Transform_m1.py'
    )
    
    python_load = BashOperator(
        task_id='python_load', 
        bash_command='sudo -u airflow python /opt/airflow/scripts/Load_m1.py'
    )

    # Menentukan ketergantungan antar task (pipeline dependency)
    python_extract >> python_transform >> python_load