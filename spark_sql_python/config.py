from pyspark.sql import SparkSession
from pydantic import Field
from pydantic_settings import BaseSettings
from pathlib import Path


class SessionInitializer(SparkSession):
    def __init__(self, app_name="Session"):
        self.session = SparkSession.builder.appName(app_name).getOrCreate()
        super().__init__(self.session.sparkContext)


spark = SessionInitializer()


class GenericSettings(BaseSettings):
    data_path: str = "Empty"
    process_step: str = "Empty"

    raw_folder: Path = Path(data_path, "raw/")
    processed_folder: Path = Path(data_path, "processed/")
    schema_folder: Path = Path(data_path, "schemas/")

    csv_type: str = "csv"
    json_type: str = "json"
    parquet_type: str = "parquet"


generic_settings = GenericSettings()
