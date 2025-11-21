from data_sync_platform.interface import GeneralConnector
import logging
import os

logger = logging.getLogger("airflow.task")

class DataSynchronizer:
    def __init__(self, source_connector: GeneralConnector, target_connector: GeneralConnector):
        self.source_connector = source_connector
        self.target_connector = target_connector

    def scan_source(self, source_path: str) -> list:
        """Just return list of files from source."""
        clean_source_path = source_path.rstrip('/')
        logging.info(f"Scanning source folder: {clean_source_path}")
        
        
        files = self.source_connector.list_files(clean_source_path)
        
        return files if files else []

    # Main processing function
    def sync_file(self, file_path: str, source_root: str, target_root: str = None, transform=None):
        """Logic sync atomic cho 1 file."""
        if target_root is None:
            target_root = source_root

        clean_source = source_root.rstrip('/')
        clean_target = target_root.rstrip('/')

       
        if not file_path.startswith(clean_source):
            logging.warning(f"Skipped: Path mismatch {file_path}")
            return

        
        relative_path = os.path.relpath(file_path, clean_source).replace('\\', '/')
        destination_path = os.path.join(clean_target, relative_path).replace('\\', '/')

        try:
            s_size = self.source_connector.get_file_size(file_path)
            t_size = self.target_connector.get_file_size(destination_path)

            if t_size != -1 and s_size == t_size:
                logging.info(f"Skipping unchanged: {relative_path}")
                return

            logging.info(f"Syncing: {relative_path} ({s_size} bytes)")

            self.target_connector.ensure_directory(destination_path)
            
            with self.source_connector.get_file_stream(file_path) as s_stream:
                in_stream = s_stream
                if transform:
                    in_stream = transform(s_stream)
                self.target_connector.save_file_stream(destination_path, in_stream)
            new_t_size = self.target_connector.get_file_size(destination_path)
            if new_t_size != s_size:
                raise Exception(f"Integrity Error: {relative_path} (Source:{s_size} != Target:{new_t_size})")

            logging.info(f"Success: {relative_path}")

        except Exception as e:
            logging.error(f"Error processing {relative_path}: {e}")
            raise e

    def close(self):
        self.source_connector.close()
        self.target_connector.close()