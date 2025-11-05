from networksecurity.components.data_ingestion import DataIngestion
from networksecurity.components.data_validation import DataValidation
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.entity.config_entity import DataIngestionConfig, DataValidationConfig, TrainingPipelineConfig
import sys

if __name__ == "__main__":
    try:
        training_pipeline_config = TrainingPipelineConfig()

        # Data Ingestion
        data_ingestion_config = DataIngestionConfig(training_pipeline_config)
        data_ingestion = DataIngestion(data_ingestion_config)
        logging.info("Initiate the data ingestion component")
        data_ingestionartifact = data_ingestion.initiate_data_ingestion()
        print(data_ingestionartifact)

        # Data Validation
        data_validation_Config = DataValidationConfig(training_pipeline_config)
        data_validation = DataValidation(data_ingestionartifact, data_validation_Config)
        logging.info("Initiate the data validation")
        data_validation_artifact = data_validation.initiate_data_validation()
        print(data_validation_artifact)
        logging.info("Data validation completed")

    except Exception as e:
        raise NetworkSecurityException(e, sys)
