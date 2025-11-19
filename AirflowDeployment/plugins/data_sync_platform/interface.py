from abc import ABC, abstractmethod
from typing import List, BinaryIO

class GeneralConnector(ABC):
    @abstractmethod
    def list_files(self, path: str) -> List[str]:
        """ List all files in folder"""
        pass
    @abstractmethod
    def get_file_stream(self, path: str) -> BinaryIO:
        """ Get file in stream mode"""
        pass
    @abstractmethod
    def save_file_stream(self,path:str, stream: BinaryIO):
        """ Save file in stream mode"""
        pass
    @abstractmethod
    def ensure_directory(self,path:str) :
        """ Ensure directory exists"""
        pass
    @abstractmethod
    def close(self):
        """ Close the connector"""
        pass