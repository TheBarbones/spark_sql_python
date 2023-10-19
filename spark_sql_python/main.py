from spark_sql_python.domain.energy.src import (
    spark_fundamentals,
    data_ingestion_schema_evolution,
    data_exploration_manipulation,
    external_data_sources,
    best_practices,
    query_optimization,
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
