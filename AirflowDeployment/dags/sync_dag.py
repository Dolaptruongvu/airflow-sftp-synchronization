from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.join(os.environ.get('AIRFLOW_HOME', '/opt/airflow'), 'plugins'))

from data_sync_platform.connectors.sftp_connector import SFTPConnector
from data_sync_platform.services.sync_service import DataSynchronizer

default_args = {
    'owner': 'airflow-DLTV',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@task
def list_files_task(source_folder: str):
    connector = SFTPConnector(ssh_conn_id='source_sftp_conn')
    try:
        target = SFTPConnector(ssh_conn_id='target_sftp_conn')
        syncer = DataSynchronizer(connector, target)
        
        return syncer.scan_source(source_folder)
    finally:
        connector.close()

@task(max_active_tis_per_dag=16)
def sync_single_file_task( source_folder: str, file_path: str):
    source = SFTPConnector(ssh_conn_id='source_sftp_conn')
    target = SFTPConnector(ssh_conn_id='target_sftp_conn')
    
    syncer = DataSynchronizer(source, target)
    try:
        
        syncer.sync_file(
            file_path=file_path, 
            source_root=source_folder
        )
    finally:
        syncer.close()

with DAG(
    dag_id='airflow_sftp_sync_dag',
    default_args=default_args,
    schedule='@daily',
    start_date=datetime(2025, 11, 20),
    catchup=False,
    tags=['data-sync', 'parallel'],
) as dag:

    SOURCE_FOLDER = 'upload' 

    files_list = list_files_task(source_folder=SOURCE_FOLDER)
    sync_single_file_task.partial(source_folder=SOURCE_FOLDER).expand(file_path=files_list)