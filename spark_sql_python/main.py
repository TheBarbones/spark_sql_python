from pyspark.sql import SparkSession
from spark_sql_python.domain.energy.src import (
    spark_fundamentals,
    data_ingestion_schema_evolution,
    data_exploration_manipulation,
    external_data_sources,
    best_practices,
    query_optimization,
)

__spark = SparkSession.builder.master("local").appName("spark_sql_python").getOrCreate()

if __name__ == "__main__":
    spark_fundamentals(__spark)
    data_ingestion_schema_evolution(__spark)
    data_exploration_manipulation(__spark)
    query_optimization(__spark)
    external_data_sources(__spark)
    best_practices(__spark)
