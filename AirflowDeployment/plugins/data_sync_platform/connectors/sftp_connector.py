from airflow.providers.sftp.hooks.sftp import SFTPHook
from data_sync_platform.interface import GeneralConnector
import os
from typing import List, BinaryIO
import stat
import logging
class SFTPConnector(GeneralConnector):
    def __init__(self, ssh_conn_id: str):
        self.hook = SFTPHook(ssh_conn_id=ssh_conn_id)
        self.sftp = self.hook.get_conn()
    
    # List all files in folder in recursive mode
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
            logging.error(f"Error listing files in {path}: {e}")
            return []
        return files
    # Get size of file
    def get_file_size(self, path: str) -> int:
        try:
            stat_info = self.sftp.stat(path)
            return stat_info.st_size
        except IOError as e:
            logging.error(f"Error getting file size for {path}: {e} - It could be that the file does not exist in target")
            return -1
    # Get file in stream mode
    def get_file_stream(self, path: str) -> BinaryIO:
        return self.sftp.open(path, 'rb')
    
    # Save file in stream mode
    def save_file_stream(self, path: str, stream: BinaryIO):
        CHUNK_SIZE = 32 * 1024 * 1024
        with self.sftp.open(path, 'wb') as target_file:
            target_file.set_pipelined(True) 
            
            while True:
                data = stream.read(CHUNK_SIZE)
                if not data:
                    break
                target_file.write(data)
            target_file.set_pipelined(False) 
            target_file.flush()

    # If directorry does not exist, create
    def ensure_directory(self, path: str):
        dirname = os.path.dirname(path)
        if not dirname or dirname == '/' or dirname == '.':
            return
        dirs_to_create = dirname.split('/')
        current_path = ""
        
        for part in dirs_to_create:
            if not part: continue 
            
            current_path = f"{current_path}/{part}" if current_path else part
            try:
                self.sftp.stat(current_path)
            except IOError:
                
                try:
                    self.sftp.mkdir(current_path)
                    logging.info(f"Created directory: {current_path}")
                # make sure another process didn't create it in the meantime
                except IOError:
                    try:
                        self.sftp.stat(current_path)
                    except IOError:
                        raise
    def close(self):
        self.sftp.close()
        self.hook.close_conn()