import sys 
import pandas as pd
from sklearn.pipeline import Pipeline

from src.exception import MyException
from src.logger import logging

class TargetValueMapping:

    def __init__(self):
        self.extrovert:int = 0
        self.introvert:int =1
    
    def _asdict(self):
        return self.__dict__
    
    def reverse_mapping(self):
        mapping_response = self._asdict()
        return dict(zip(mapping_response.values(),mapping_response.keys()))
    
class MyModel:
    def __init__(self,preprocessing_object:Pipeline,trained_model_object:Pipeline):
        """
        :param preprocessing_object: Input Object of preprocessor
        :param trained_model_object: Input Object of trained model
        """

        self.preprocessing_object = preprocessing_object
        self.trained_model_object = trained_model_object
    
    def predict(self,dataframe: pd.DataFrame):
        """
        Function accepts preprocessed inputs (with all custom transformation already applied)
        applies scaling using preprocessing_object, and performs prediction of transformed features
        """
        try:
            logging.info("Starting prediction process.")

            # step 1: Applying scaling transformation using the pre-trained processing object
            transformed_object = self.preprocessing_object.transform(dataframe)

            # step 2: perform prediction using the trained model
            logging.info("Using the trained model to get prediction")
            predictions = self.trained_model_object.predict(transformed_object)
            return predictions
        except Exception as e:
            logging.error("Error occurred in predict method",exc_info=True)
            raise MyException(e, sys) from e 
    
    def __repr__(self):
        return f"{type(self.trained_model_object).__name__}()"
    def __str__(self):
        return f"{type(self.trained_model_object).__name__}()"

