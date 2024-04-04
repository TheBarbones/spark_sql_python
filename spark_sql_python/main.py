from spark_sql_python.src import (
    SparkFundamentals,
    data_ingestion_schema_evolution,
    data_exploration_manipulation,
)
from spark_sql_python.config import generic_settings


def main():
    spark = generic_settings.spark.session
    if generic_settings.process_step == "a":
        SparkFundamentals(spark)
    elif generic_settings.process_step == "b":
        data_ingestion_schema_evolution.schema_evolution(spark)
    elif generic_settings.process_step == "c":
        data_exploration_manipulation(spark)
    else:
        pass
    # query_optimization(spark)
    # external_data_sources(spark)
    # best_practices(spark)


if __name__ == "__main__":
    main()
