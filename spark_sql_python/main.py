from spark_sql_python.src import (
    spark_fundamentals,
    data_ingestion_schema_evolution,
)
from spark_sql_python.config import generic_settings


def main():
    spark = generic_settings.spark.session
    spark_fundamentals(spark)
    data_ingestion_schema_evolution.schema_evolution(spark)
    # data_exploration_manipulation(spark)
    # query_optimization(spark)
    # external_data_sources(spark)
    # best_practices(spark)


if __name__ == "__main__":
    main()
