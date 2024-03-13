from finance_complaint.exception import FinanceException
from finance_complaint.logger import logging
from finance_complaint.utils import read_yaml_file, write_yaml_file
import os,sys
from collections import namedtuple

DataIngestionMetadataInfo = namedtuple('DataIngestionMetadataInfo',
                                       ['from_date', 'to_date', 'file_path'])

class DataIngestionMetaData:

    def __init__(self, metadata_file_path):
        self.metadata_file_path = metadata_file_path

    @property
    def is_metadata_file_present(self)->bool:
        return os.path.exists(self.metadata_file_path)
    
    def write_metadata_info(self, from_data:str, to_data:str, data_file_path:str):
        try:
            metadata_info = DataIngestionMetadataInfo(from_date=from_data,
                                                      to_date=to_data,
                                                      data_file_path=data_file_path)
            write_yaml_file(file_path=self.metadata_file_path,
                            data=metadata_info._asdict())
            
        except Exception as e:
            raise FinanceException(e, sys)
        
    def get_metadata_info(self)->DataIngestionMetadataInfo:
        try:
            if not self.is_metadata_file_present:
                raise Exception('No MetaData file present')
            metadata = read_yaml_file(self.metadata_file_path)
            metadata_info = DataIngestionMetadataInfo(**(metadata))
            logging.info(metadata)
            return metadata_info
        
        except Exception as e:
            raise FinanceException(e, sys)
