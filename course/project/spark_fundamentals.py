from pyspark.sql import DataFrame
from pyspark.sql import SparkSession, Row
from course.config.sparkSession import spark, loading_data


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
        
Basic sql operations
    - Registering Dataframes as tables
    - Running SQL Queries
    - Aggregations
    - Filtering and Sorting
    - Joining Data
DataFrame API vs. Spark SQL

"""


def create_dataframe(spark: SparkSession):
    foods = Row("id", "amount", "date", "description", 'people_id')
    services = Row('id', 'amount', 'date', 'description', 'people_id')
    people = Row('id', 'name', 'birth')

    food_df = spark.createDataFrame(
        [
            foods(121, 240.00, "2023-08-10", "carrot", '23'),
            foods(122, 140.00, "2023-08-11", "banana", '23'),
            foods(123, 40.00, "2023-08-12", "cinnamon", '22'),
            foods(124, 340.00, "2023-08-13", "potato", '21'),
            foods(125, 45.00, "2023-08-13", "sugar", '22'),
            foods(126, 200.00, "2023-08-14", "cucumber", '21'),
        ]
    )

    service_df = spark.createDataFrame(
        [
            services(321, 240.00, "2023-08-10", "3d printer", '21'),
            services(322, 140.00, "2023-08-11", "cinema", '22'),
            services(323, 40.00, "2023-08-12", "cinema", '23'),
            services(324, 340.00, "2023-08-13", "swimming", '22'),
            services(325, 45.00, "2023-08-13", "house cleaning", '21'),
            services(326, 200.00, "2023-08-14", "delivery food", '21'),
        ]
    )

    people_df = spark.createDataFrame(
        [
            people(21, 'mary', "1990-07-10"),
            people(22, 'kat', "2000-04-11"),
            people(23, 'bianca', "1989-08-12")
        ]
    )

    output_list = [food_df, service_df, people_df]
    return output_list


def registering_dataframes_as_tables(list_input: list[DataFrame]):
    """
    Create a temporary view in memory.

    :param list_input: list of dataframes
    :return: None
    """
    list_input[0].createTempView("foods_view")
    list_input[1].createTempView("services_view")
    list_input[2].createTempView("people_view")


def running_sql_queries(spark: SparkSession):
    """
    Execution generic sql sentences

    :param spark: Spark session
    :return: None
    """
    temp_df = spark.sql("select id, amount, date, description from foods_view")


def aggregations(spark: DataFrame):
    """
    Aggregation operation to find avg, min, max, sum, count and others

    :param spark: Spark session
    :return: None
    """
    agg_df = spark.sql("SELECT sum(amount), date FROM temp_view GROUP BY date")
    agg_df.show()


def filtering_and_sorting(spark: DataFrame):
    """
    Filtering operations to get accurated values as much as we need
    Sorting operation to order our result

    :param spark: Spark session
    :return: None
    """
    filtered_df = spark.sql("SELECT * FROM temp_view WHERE date >= '2023-08-13'")
    filtered_df.show()

    sorting_df = spark.sql(
        "SELECT * FROM temp_view WHERE date >= '2023-08-13' ORDER BY id"
    )
    sorting_df.show()


def joining_data(spark: DataFrame):
    """
    Joinin operations such inner, left, right and others

    :param spark: Spark session
    :return: None
    """
    joining_df = spark.sql(
        "SELECT * FROM temp_view "
        "JOIN parquet_view ON csv_view.id = parquet_view.id "
        "WHERE parquet_view.species != 'setosa' ORDER BY csv_view.id"
    )

    joining_df.show()


def writing_data(spark: DataFrame):
    """
    Write operations and options

    :param spark: Spark session
    :return: None
    """
    writing_df = spark.sql("SELECT id, amount FROM temp_view")
    writing_df.write.mode("overwrite").parquet("course/data/output/write_exercise/")


df_list = create_dataframe(spark)
registering_dataframes_as_tables(initial_df)
running_sql_queries(spark)
aggregations(spark)
filtering_and_sorting(spark)
joining_data(spark)
writing_data(spark)
caching_data(spark)
