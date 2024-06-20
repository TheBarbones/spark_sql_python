from spark_sql_python.src.domain import SparkFundamentals, DataIngestionSchemaEvolution
from spark_sql_python.config import generic_settings, spark


def main():
    spark_fundamentals = SparkFundamentals(spark)
    schema_evolution = DataIngestionSchemaEvolution(spark)
    match generic_settings.PROCESS_STEP:
        case generic_settings.fundamentals:
            spark_fundamentals.dataframe_api_spark_sql()
        case generic_settings.ingestion:
            schema_evolution.schema_evolution()
        case generic_settings.exploration:
            print("exploration")
        case generic_settings.optimization:
            print("optimization")
        case generic_settings.external:
            print("extener")


if __name__ == "__main__":
    main()
