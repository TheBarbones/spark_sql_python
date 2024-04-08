from pyspark.sql import SparkSession
from pydantic import Field
import os


class SessionInitializer(SparkSession):
    # def __init__(self, app_name="Session"):
    #     self.session = SparkSession.builder.appName(app_name).getOrCreate()
    def pyspark(self):
        return self.builder.appName("Session").getOrCreate()


spark = SessionInitializer.pyspark(SparkSession)


class GenericSettings:
    def __init__(self):
        # self.spark = SparkSessionInitializer()
        # self.energy_data_path = os.environ["_DATA_PATH_"]
        self._data_path_ = Field(env="_DATA_PATH_")
        self._step_ = Field(env="_COURSE_PART_")
        self.raw_folder = "raw/"
        self.processed_folder = "processed/"
        self.schema_folder = "schemas/"
        self.biofuel_production_file = "biofuel-production"
        self.csv_type = "csv"
        self.json_type = "json"
        self.parquet_type = "parquet"


generic_settings = GenericSettings()
