from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.join(os.environ.get('AIRFLOW_HOME', '/opt/airflow'), 'plugins', 'data_sync_platform'))

from data_sync_platform.connectors.sftp_connector import SFTPConnector
from data_sync_platform.services.sync_service import DataSynchronizer

default_args = {
    'owner': 'airflow-DLTV',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_sync():
    source_connector = SFTPConnector(ssh_conn_id='source_sftp_conn')
    target_connector = SFTPConnector(ssh_conn_id='target_sftp_conn')

    syncer = DataSynchronizer(source_connector, target_connector)
    try:
        syncer.sync_folder('/source', '/source')

    finally:
        syncer.close()

with DAG(
    dag_id='airflow_sftp_sync_dag',
    default_args=default_args,
    schedule='@daily',
    start_date=datetime(2025, 11, 21),
    catchup=False,
    tags=['data-sync'],
) as dag:
    sync_task = PythonOperator(
        task_id='sftp_data_sync',
        python_callable=run_sync,
    )

    sync_task