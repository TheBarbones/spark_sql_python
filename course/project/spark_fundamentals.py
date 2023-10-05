from pyspark.sql import DataFrame
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import lit, col
from course.config.sparkSession import spark


"""
Spark SQL:
    - Work with structure (Csv, Parquet, etc) and semi-structured data (Json, Xml)
    - Support SQL-like sintax or SQL ANSI
    - Work with Spark's distributed computing capabilities
    - Provide interface to work with MLib, GraphX and Streaming

Dataframe API:
    - Interface more friendly than RDD API
    - Operations similar than SQL
    - Support wide range of data formats: Csv, Parquet, Json, Orc, etc
    
Differences:
    - Execution engine: 
        - Spark SQL: Uses a query optimizer and an execution engine
        - Dataframe API: Relies on Spark's RDD execution engine
    - Code generation:
        - Spark SQL: Generate bytecode at runtime
        - Dataframe API: Relies on the JVM
    - SQL-like syntax:
        - Spark SQL: Syntax with SQL-like, similar to SQL ANSI
        - Dataframe API: Functional programming constructs
        
"""


def create_dataframe(spark: SparkSession):
    foods = Row("id", "amount", "date", "description", "people_id")
    services = Row("id", "amount", "date", "description", "people_id")
    people = Row("id", "name", "birth")

    food_df = spark.createDataFrame(
        [
            foods(121, 240.00, "2023-08-10", "carrot", "23"),
            foods(122, 140.00, "2023-08-11", "banana", "23"),
            foods(123, 40.00, "2023-08-12", "cinnamon", "22"),
            foods(124, 340.00, "2023-08-13", "potato", "21"),
            foods(125, 45.00, "2023-08-13", "sugar", "22"),
            foods(126, 200.00, "2023-08-14", "cucumber", "21"),
        ]
    )

    service_df = spark.createDataFrame(
        [
            services(321, 240.00, "2023-08-10", "3d printer", "21"),
            services(322, 140.00, "2023-08-11", "cinema", "22"),
            services(323, 40.00, "2023-08-12", "cinema", "23"),
            services(324, 340.00, "2023-08-13", "swimming", "22"),
            services(325, 45.00, "2023-08-13", "house cleaning", "21"),
            services(326, 200.00, "2023-08-14", "delivery food", "21"),
        ]
    )

    people_df = spark.createDataFrame(
        [
            people(21, "mary", "1990-07-10"),
            people(22, "kat", "2000-04-11"),
            people(23, "bianca", "1989-08-12"),
        ]
    )

    output_list = [food_df, service_df, people_df]
    return output_list


def basic_operations(list_input: list[DataFrame]):
    """
    Basic sql operations
    - Registering Dataframes as tables
    - Running SQL Queries
        - Simple select
        - Aggregations
        - Filtering and Sorting
        - Joining Data

    :param list_input: list of dataframes
    :return: None
    """
    # registering dataframe as temporal tables
    list_input[0].createTempView("foods_view")
    list_input[1].createTempView("services_view")
    list_input[2].createTempView("people_view")

    ## Simple select

    # Spark SQL
    foods_df = spark.sql("select id, amount, date, description from foods_view")

    # Dataframe API
    foods_df = list_input[0].select(
        col("id"), col("amount"), col("date"), col("description")
    )

    ## Aggregations

    # Spark SQL
    agg_df = spark.sql("SELECT sum(amount), date FROM temp_view GROUP BY date")

    # Dataframe API
    agg_df = (
        list_input[2]
        .groupby(col("species"))
        .agg(
            {
                "sepal_length": "max",
                "sepal_width": "min",
                "petal_length": "count",
                "petal_width": "avg",
            }
        )
    )

    ## Filtering

    # Spark SQL
    filtered_df = spark.sql("SELECT * FROM temp_view WHERE date >= '2023-08-13'")

    # Dataframe API
    filtered_df = list_input[2].filter(col("species") == lit("setosa"))

    ## Sorting

    # Spark SQL
    sorting_df = spark.sql(
        "SELECT * FROM temp_view WHERE date >= '2023-08-13' ORDER BY id"
    )

    # Dataframe API
    sorting_df = list_input[2].orderBy(col("date").desc)

    ## Joining operations

    # Spark SQL
    joining_df = spark.sql(
        "SELECT * FROM temp_view "
        "JOIN parquet_view ON csv_view.id = parquet_view.id "
        "WHERE parquet_view.species != 'setosa' ORDER BY csv_view.id"
    )

    # Dataframe API
    joining_one_df = list_input[0]
    joining_two_df = list_input[2]

    joining_data_df = joining_one_df.alias("j1").join(
        joining_two_df.alias("j2"), col("j1.id") == col("j2.id"), "inner"
    )

    joining_data_df = joining_one_df.join(joining_two_df, ["id"], "inner")

    condition = [col("j1.id") == col("j2.id")]
    joining_data_df = joining_one_df.alias("j1").join(
        joining_two_df.alias("j2"), condition, "left"
    )


df_list = create_dataframe(spark)
basic_operations(df_list)
