from pyspark.sql import SparkSession


class SparkSessionInitializer:
    def __init__(self, app_name="Session"):
        self.session = SparkSession.builder.appName(app_name).getOrCreate()


class GenericSettings:
    def __init__(self):
        self.spark = SparkSessionInitializer()
        self.energy_data_path = "spark_sql_python/domain/energy/data/"
        self.raw_folder = "raw/"
        self.processed_folder = "processed/"
        self.schema_folder = "schemas/"
        self.biofuel_production_file = "biofuel-production"
        self.csv_type = "csv"
        self.json_type = "json"
        self.parquet_type = "parquet"


generic_settings = GenericSettings()
