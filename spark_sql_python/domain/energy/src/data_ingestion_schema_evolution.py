from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
)
import json
from spark_sql_python.config import generic_settings
from spark_sql_python.common import (
    read_file_with_schema,
    get_schema_file,
    empty_dataframe,
)

"""
Data ingestion & Schema evolution
---------------------------------
    ✨ Loading data from various sources
    ✨ Handing schema evolution and data type inference
    ✨ Schema customization and enforcement

Sources:
    - Csv, Text, Json, Parquet, 

Schemas:
    - StructType & StructField, schema_of_json, 
    
Schema Evolution:
    Schema evolution is a feature that allows users to easily change a table's current schema to accommodate data that is 
    changing over time. Most commonly, it's used when performing an append or overwrite operation, to automatically adapt 
    the schema to include one or more new columns.

Schema Enforcement:
    Schema enforcement, also known as schema validation, is a safeguard in Delta Lake that ensures data quality by rejecting 
    writes to a table that do not match the table's schema. Like the front desk manager at a busy restaurant that only accepts
     reservations, it checks to see whether each column in data inserted into the table is on its list of expected columns
      (in other words, whether each one has a "reservation"), and rejects any writes with columns that aren't on the list.




"""


class DataIngestionSchemaEvolution:
    def __init__(self):
        self.biofuel_production = empty_dataframe

    def loading_data(self, spark: SparkSession):
        """

        :param spark:
        :return:
        """

        schema = get_schema_file(file_name=generic_settings.biofuel_production_file)
        self.biofuel_production = read_file_with_schema(
            spark=spark,
            file_name=generic_settings.biofuel_production_file,
            file_type=generic_settings.csv_type,
            schema_file=schema,
        )

        self.biofuel_production.show()

        return self.biofuel_production

    def schema_evolution(self, spark: SparkSession):
        list_input = self.loading_data(spark)

        # Add the mergeSchema option

    # # schema enforcement
    # loans.write.format("delta").mode("append").save(DELTALAKE_PATH)
    #
    # # schema evolution
    # loans.write.format("delta").option("mergeSchema", "true").mode("append").save(
    #     DELTALAKE_SILVER_PATH
    # )


data_ingestion_schema_evolution = DataIngestionSchemaEvolution()
