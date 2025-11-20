from data_sync_platform.interface import GeneralConnector
import logging
import os

class DataSynchronizer:
    def __init__(self, source_connector: GeneralConnector, target_connector: GeneralConnector):
        self.source_connector = source_connector
        self.target_connector = target_connector
    def sync_folder(self, source_path: str, target_path: str = None, transform=None):

        if target_path is None:
            target_path = source_path
        
        """ delete trailing slash"""
        clean_source_path = source_path.rstrip('/')
        clean_target_path = target_path.rstrip('/')
        logging.info(f"Scanning source folder: {clean_source_path}")

        source_files = self.source_connector.list_files(clean_source_path)
        logging.info(f"Found {len(source_files)} files in source folder.")

        skipped_count = 0
        synced_count = 0

        for file_path in source_files:
            if not file_path.startswith(clean_source_path):
                logging.warning(f"Skipping file with unexpected path: {file_path}")
                continue
            
            relative_path = os.path.relpath(file_path, clean_source_path).replace('\\', '/')
            destination_path = os.path.join(clean_target_path, relative_path).replace('\\', '/')

            try:
                source_size = self.source_connector.get_file_size(file_path)
                target_size = self.target_connector.get_file_size(destination_path)
                if source_size != -1 and source_size == target_size:
                    logging.info(f"Skipping unchanged file: {relative_path}")
                    skipped_count += 1
                    continue
                logging.info(f"Syncing file: {relative_path}")

                self.target_connector.ensure_directory(destination_path)

                with self.source_connector.get_file_stream(file_path) as source_stream:
                    input_stream = source_stream
                    if transform:
                        input_stream = transform(source_stream)
                    self.target_connector.save_file_stream(destination_path, input_stream)
                synced_count += 1
            except Exception as e:
                logging.error(f"Error syncing file {relative_path}: {e}")
                continue
        logging.info(f"{synced_count} files synced, {skipped_count} files skipped.")
    def close(self):
        self.source_connector.close()
        self.target_connector.close()