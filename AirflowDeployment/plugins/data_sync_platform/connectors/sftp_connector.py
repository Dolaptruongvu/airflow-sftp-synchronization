from airflow.providers.sftp.hooks.sftp import SFTPHook
from data_sync_platform.interface import GeneralConnector
import os
from typing import List, BinaryIO
import stat

class SFTPConnector(GeneralConnector):
    def __init__(self, ssh_conn_id: str):
        self.hook = SFTPHook(ssh_conn_id=ssh_conn_id)
        self.sftp = self.hook.get_conn()
    
    def list_files(self, path: str) -> List[str]:
        files = []
        try: 
            clean_base_path = path.rstrip('/')
            """handle root path case"""
            if clean_base_path == '':
                clean_base_path = '/'
            def walk(current_path):
                for entry in self.sftp.listdir_attr(current_path):
                    full_path = os.path.join(current_path, entry.filename).replace('\\', '/')
                    """if it is directory"""
                    if stat.S_ISDIR(entry.st_mode):
                        walk(full_path)
                    else:
                        files.append(full_path)
            walk(clean_base_path)
        except Exception as e:
            print(f"Error listing files in {path}: {e}")
            return []
        return files
    def get_file_size(self, path: str) -> int:
        try:
            stat_info = self.sftp.stat(path)
            return stat_info.st_size
        except IOError as e:
            print(f"Error getting file size for {path}: {e}")
            return -1
    
    def get_file_stream(self, path: str) -> BinaryIO:
        return self.sftp.open(path, 'rb')
    
    def save_file_stream(self, path: str, stream: BinaryIO):
        self.sftp.putfo(fl=stream, remotepath=path)

    def ensure_directory(self, path: str):
        dirname = os.path.dirname(path)
        if dirname:
            try:
                self.hook.create_directory(dirname)
            except Exception:
                pass
    def close(self):
        self.sftp.close()
        self.hook.close_conn()