from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

"""
Data ingestion & Schema evolution
---------------------------------
    ✨ Loading data from various sources
    ✨ Handing schema evolution and data type inference
    ✨ Schema customization and enforcement

Sources:
    - Csv, Text, Json, Parquet, 
Options CSV:
    - header, inferSchema, delimiter, quote, escape, multiline, ignoreLeadingWhiteSpace, ignoreTrailingWhiteSpace
    
Options Text:
    - 

Options JSON:
    -
    
Options Parquet:
    -

Schemas:
    - StructType & StructField, schema_of_json, 
    
Schema Evolution:
Schema evolution is a feature that allows users to easily change a table's current schema to accommodate data that is changing over time. Most commonly, it's used when performing an append or overwrite operation, to automatically adapt the schema to include one or more new columns.

Schema Enforcement:
Schema enforcement, also known as schema validation, is a safeguard in Delta Lake that ensures data quality by rejecting writes to a table that do not match the table's schema. Like the front desk manager at a busy restaurant that only accepts reservations, it checks to see whether each column in data inserted into the table is on its list of expected columns (in other words, whether each one has a "reservation"), and rejects any writes with columns that aren't on the list.




"""


def loading_data(spark: SparkSession):
    """

    :param spark:
    :return:
    """
    parm_path = "course/data/input/"
    parm_file = "iris_2023"

    # Reading CSV files
    csv_df = spark.read.csv(f"{parm_path}{parm_file}.xls")

    options_csv_dict = {"header": True, "inferSchema": True}

    csv_df = spark.read.options(options_csv_dict).csv(f"{parm_path}{parm_file}.xls")
    csv_df.printSchema()

    # Reading JSON files
    json_df = spark.read.option("header", "true").json(f"{parm_path}{parm_file}.json")

    # Reading PARQUET files
    parquet_df = spark.read.option("header", "true").parquet(
        f"{parm_path}iris_2023.parquet"
    )

    return [csv_df, json_df, parquet_df]


def data_ingestion_schema_evolution(spark: SparkSession):
    list_input = loading_data(spark)

    csv_df = list_input[0]
    json_df = list_input[1]
    parquet_df = list_input[2]

    # Add the mergeSchema option


# # schema enforcement
# loans.write.format("delta").mode("append").save(DELTALAKE_PATH)
#
# # schema evolution
# loans.write.format("delta").option("mergeSchema", "true").mode("append").save(
#     DELTALAKE_SILVER_PATH
# )
