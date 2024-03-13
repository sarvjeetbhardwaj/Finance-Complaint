from finance_complaint.exception import FinanceException
from finance_complaint.logger import logging as  logger
from finance_complaint.entity.config_entity import (DataIngestionConfig,TrainingPipelineConfig)
from finance_complaint.entity.artifact_entity import (DataIngestionArtifact)
from finance_complaint.component.data_ingestion import DataIngestion
import sys

class TrainingPipeline:

    
    def __init__(self, training_pipeline_config: TrainingPipelineConfig):
        self.training_pipeline_config: TrainingPipelineConfig = training_pipeline_config

    def start_data_ingestion(self) -> DataIngestionArtifact:
        try:
            data_ingestion_config = DataIngestionConfig(training_pipeline_config=self.training_pipeline_config)
            data_ingestion = DataIngestion(data_ingestion_config=data_ingestion_config)
            data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
            return data_ingestion_artifact

        except Exception as e:
            raise FinanceException(e, sys)
        
    def start(self):
        try:
            data_ingestion_artifact = self.start_data_ingestion()
        except Exception as e:
            raise FinanceException(e, sys)
