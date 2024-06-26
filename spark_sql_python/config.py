from pyspark.sql import SparkSession
from pydantic_settings import BaseSettings
from pathlib import Path


class SessionInitializer(SparkSession):
    def __init__(self, app_name="SparkCourse"):
        self.session = SparkSession.builder.appName(app_name).getOrCreate()
        super().__init__(self.session.sparkContext)


spark = SessionInitializer()


class GenericSettings(BaseSettings):
    DATA_PATH: str = "Empty"
    PROCESS_STEP: str = "Empty"

    raw_folder: Path = Path(DATA_PATH, "raw/")
    processed_folder: Path = Path(DATA_PATH, "processed/")
    schema_folder: Path = Path(DATA_PATH, "schemas/")

    csv_type: str = "csv"
    json_type: str = "json"
    parquet_type: str = "parquet"

    fundamentals: str = "fundamentals"
    ingestion: str = "ingestion"
    exploration: str = "exploration"
    optimization: str = "optimization"
    external: str = "external"


generic_settings = GenericSettings()
