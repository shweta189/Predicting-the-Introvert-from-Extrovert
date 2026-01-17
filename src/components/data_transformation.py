import sys
import numpy as np 
import pandas as pd 
from pandas import DataFrame
from sklearn.preprocessing import StandardScaler ,LabelEncoder,OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from imblearn.over_sampling import SMOTE

from src.constants import TARGET_COLUMN,SCHEMA_FILE_PATH
from src.entity.config_entity import DataTransformationConfig
from src.entity.artifact_entity import DataTransformationArtifact, DataIngestionArtifact, DataValidationArtifact
from src.exception import MyException
from src.logger import logging
from src.utils.main_utils import save_object, save_numpy_array_data, read_yaml_file


class DataTransformation:
    def __init__(self, data_ingestion_artifact: DataIngestionArtifact, data_validation_artifact: DataValidationArtifact, data_transformation_config: DataTransformationConfig):
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_artifact = data_validation_artifact
            self.data_transformation_config = data_transformation_config
            self._schema_config = read_yaml_file(SCHEMA_FILE_PATH)
        except Exception as e:
            raise MyException(e, sys)
        
    
    @staticmethod
    def read_data(file_path:str) -> pd.DataFrame:
        try:
            column_names = ['Time_spent_Alone', 'Stage_fear', 'Social_event_attendance', 
                           'Going_outside', 'Drained_after_socializing', 'Friends_circle_size', 
                           'Post_frequency', 'Personality']
            return pd.read_csv(file_path,names=column_names,header=0)
        except Exception as e:
            raise MyException(e, sys)
        
    def get_numerical_and_categorical_features(self) -> tuple:

        try:
            numerical_features = self._schema_config.get('num_features',[])
            categorical_features = self._schema_config.get('cat_features',[])

            if TARGET_COLUMN in categorical_features:
                categorical_features.remove(TARGET_COLUMN)
            logging.info(f"Numerical features : {numerical_features}")
            logging.info(f"categorical_features: {categorical_features}")

            return numerical_features, categorical_features
        except Exception as e:
            raise MyException(e,sys)
        
    def handle_outliers(self,dataframe:DataFrame, num_features:list) -> DataFrame:
        try:
            logging.info("Handling outliers using IQR method")
            df = dataframe.copy()

            for col in num_features:
                if col in df.columns:
                    q1 = df[col].quantile(0.25)
                    q3 = df[col].quantile(0.75)
                    iqr = q3 -q1 
                    lower_bound = q1 - 1.5* iqr 
                    upper_bound = q3 + 1.5* iqr

                    df[col] = df[col].clip(lower= lower_bound,upper=upper_bound)
            logging.info(f"Outliers handled for numerical columns: {num_features}")
        except Exception as e:
            raise MyException(e,sys) from e 
        
    def initiate_data_transformation(self) -> DataTransformationArtifact:
        try:
            logging.info("Starting data transformation")
            train_df = self.read_data(file_path=self.data_ingestion_artifact.trained_file_path)
            test_df = self.read_data(file_path=self.data_ingestion_artifact.test_file_path)

            logging.info(f"Train dataframe shape: {train_df.shape}")
            logging.info(f"Test dataframe shape: {test_df.shape}")

            numerical_features, categorical_features = self.get_numerical_and_categorical_features()
            
            input_feature_train_df = train_df.drop(columns=[TARGET_COLUMN],axis=1)
            target_feature_train_df = train_df[TARGET_COLUMN]

            input_feature_test_df = test_df.drop(columns=[TARGET_COLUMN],axis=1)
            target_feature_test_df = test_df[TARGET_COLUMN]

            encoder = LabelEncoder()
            target_feature_train_enc = encoder.fit_transform(target_feature_train_df)
            target_feature_test_enc = encoder.transform(target_feature_test_df)

            logging.info("Encoded Target Column for Train-Test DataFrame")

            num_pipeline = Pipeline([
                ('imputer',SimpleImputer(strategy='median')),
                ('scaler',StandardScaler())
            ])
            cat_pipeline = Pipeline([
                ("imputer",SimpleImputer(strategy='most_frequent')),
                ("ohe",OneHotEncoder(handle_unknown='ignore',sparse_output=False))
            ])

            preprocessor = ColumnTransformer(
                transformers = [
                    ("num",num_pipeline,numerical_features),
                    ('cat',cat_pipeline,categorical_features),
                ]
            )
            logging.info("Imputed the missing values for numerical and categorical features")

            logging.info("Applying preprocessing pipeline")
            input_feature_train_arr = preprocessor.fit_transform(input_feature_train_df)
            input_feature_test_arr= preprocessor.transform(input_feature_test_df)

            logging.info("Applying SMOTE to training data")
            smt = SMOTE(random_state=42)
            input_feature_train_final, target_feature_train_final = smt.fit_resample(input_feature_train_arr,target_feature_train_enc)
            input_feature_test_final =input_feature_test_arr
            target_feature_test_final = target_feature_test_enc

            train_arr = np.c_[input_feature_train_final,np.array(target_feature_train_final)]
            test_arr = np.c_[input_feature_test_final,np.array(target_feature_test_final)]
            logging.info("feature-target concatenation done for train-test df.")
            save_object(self.data_transformation_config.transformed_object_file_path,preprocessor)
            save_numpy_array_data(self.data_transformation_config.transformed_train_file_path,array=train_arr)
            save_numpy_array_data(self.data_transformation_config.transformed_test_file_path,array=test_arr)
            logging.info("Saving transformation object and transformed file data")

            logging.info("Data Transformation Completed Successfully")

            return DataTransformationArtifact(
                transformed_object_file_path=self.data_transformation_config.transformed_object_file_path,
                transformed_train_file_path= self.data_transformation_config.transformed_train_file_path,
                transformed_test_file_path= self.data_transformation_config.transformed_test_file_path
            )
        except Exception as e:
            raise MyException(e, sys) from e

