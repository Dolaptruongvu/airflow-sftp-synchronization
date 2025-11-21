from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from data_sync_platform.interface import GeneralConnector
from typing import List, BinaryIO

class S3Connector(GeneralConnector):
    def __init__(self, aws_conn_id: str, bucket_name: str):
        self.hook = S3Hook(aws_conn_id=aws_conn_id)
        self.bucket = bucket_name

    def list_files(self, path: str) -> List[str]:
        keys = self.hook.list_keys(bucket_name=self.bucket, prefix=path)
        return keys if keys else []

    def get_file_stream(self, path: str) -> BinaryIO:
        obj = self.hook.get_key(key=path, bucket_name=self.bucket)
        return obj.get()['Body']

    def save_file_stream(self, path: str, stream: BinaryIO):
        self.hook.load_file_obj(stream, key=path, bucket_name=self.bucket, replace=True)

    def ensure_directory(self, path: str):
        pass

    def get_file_size(self, path: str) -> int:
        obj = self.hook.get_key(key=path, bucket_name=self.bucket)
        return obj.content_length if obj else -1

    def close(self):
        pass 