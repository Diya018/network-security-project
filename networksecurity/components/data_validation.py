import sys
import os
import pandas as pd
from scipy.stats import ks_2samp
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.entity.config_entity import DataValidationConfig
from networksecurity.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
from networksecurity.constant.training_pipeline import SCHEMA_FILE_PATH
from networksecurity.utils.main_utils.utils import read_yaml_file, write_yaml_file


class DataValidation:
    def __init__(self, data_ingestion_artifact: DataIngestionArtifact,
                 data_validation_config: DataValidationConfig):
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self.schema_config = read_yaml_file(SCHEMA_FILE_PATH)

        except Exception as e:
            raise NetworkSecurityException(e, sys)

    @staticmethod
    def read_data(filepath) -> pd.DataFrame:
        try:
            return pd.read_csv(filepath)
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def validate_no_of_columns(self, dataframe: pd.DataFrame) -> bool:
        try:
            required_columns = self.schema_config["columns"]  # ✅ FIXED
            number_of_columns = len(required_columns)

            logging.info(f"✅ Required columns ({number_of_columns}): {required_columns}")
            logging.info(f"📄 DataFrame columns ({len(dataframe.columns)}): {list(dataframe.columns)}")

            if number_of_columns == len(dataframe.columns):
                logging.info("✅ Column count matches schema.")
                return True
            else:
                logging.warning("⚠️ Column count does NOT match schema.")
                return False

        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def detect_dataset_drift(self, base_df: pd.DataFrame, current_df: pd.DataFrame, threshold=0.05) -> bool:
        try:
            status = True
            report = {}

            for column in base_df.columns:
                d1 = base_df[column]
                d2 = current_df[column]
                test_result = ks_2samp(d1, d2)
                drift_found = test_result.pvalue < threshold

                if drift_found:
                    status = False  # drift detected

                report[column] = {
                    "p_value": float(test_result.pvalue),
                    "drift_status": drift_found
                }

            # save drift report
            drift_report_file_path = self.data_validation_config.drift_report_file_path
            os.makedirs(os.path.dirname(drift_report_file_path), exist_ok=True)
            write_yaml_file(filepath=drift_report_file_path, content=report)

            logging.info(f"✅ Drift report saved at: {drift_report_file_path}")
            return status

        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def initiate_data_validation(self) -> DataValidationArtifact:
        try:
            train_file_path = self.data_ingestion_artifact.train_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path

            # read train/test data
            train_df = self.read_data(train_file_path)
            test_df = self.read_data(test_file_path)

            # validate column count
            error_message = ""
            if not self.validate_no_of_columns(train_df):
                error_message += "Train dataframe does not contain all columns. "
            if not self.validate_no_of_columns(test_df):
                error_message += "Test dataframe does not contain all columns. "

            if error_message:
                raise NetworkSecurityException(error_message, sys)

            # detect dataset drift
            validation_status = self.detect_dataset_drift(base_df=train_df, current_df=test_df)

            # save valid data
            os.makedirs(os.path.dirname(self.data_validation_config.valid_train_file_path), exist_ok=True)
            train_df.to_csv(self.data_validation_config.valid_train_file_path, index=False, header=True)
            test_df.to_csv(self.data_validation_config.valid_test_file_path, index=False, header=True)

            data_validation_artifact = DataValidationArtifact(
                validation_status=validation_status,
                valid_train_file_path=self.data_validation_config.valid_train_file_path,
                valid_test_file_path=self.data_validation_config.valid_test_file_path,
                invalid_train_file_path=None,
                invalid_test_file_path=None,
                drift_report_file_path=self.data_validation_config.drift_report_file_path
            )

            logging.info("✅ Data validation completed successfully.")
            return data_validation_artifact

        except Exception as e:
            raise NetworkSecurityException(e, sys)
