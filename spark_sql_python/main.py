from spark_sql_python.src.domain import (
    SparkFundamentals,
    data_ingestion_schema_evolution,
    # data_exploration_manipulation,
)
from spark_sql_python.config import generic_settings, spark


def main():
    match generic_settings.PROCESS_STEP:
        case generic_settings.fundamentals:
            SparkFundamentals.dataframe_api_spark_sql(spark)
        case generic_settings.ingestion:
            data_ingestion_schema_evolution.schema_evolution(spark)
        case generic_settings.exploration:
            print("exploration")
        case generic_settings.optimization:
            print("optimization")
        case generic_settings.external:
            print("extener")


if __name__ == "__main__":
    main()
