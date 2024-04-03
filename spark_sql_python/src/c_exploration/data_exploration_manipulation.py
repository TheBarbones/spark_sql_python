from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

"""
Data exploration & Manipulation
-------------------------------
    ✨ Using SQL for data exploration
    ✨ Filtering, selecting, and aggregation data
    ✨ Join operations and window functions
"""


def data_exploration_manipulation(spark: SparkSession):
    """
    Basic sql operations
    - Registering Dataframes as temporal tables
    - Running SQL Queries
        - Simple select
        - Aggregations
        - Filtering and Sorting
        - Joining Data

    :param spark: spark session
    :return: None
    """

    dataframe_list = create_dataframe(spark)
    food_df = dataframe_list[0]
    # service_df = dataframe_list[1]
    people_df = dataframe_list[1]

    """
    We are going to create a temporal view from dataframe
        - foods_view => food_df
        - services_view => service_df
        - people_view => people_df
    """

    food_df.createTempView("foods_view")
    # service_df.createTempView("services_view")
    people_df.createTempView("people_view")

    """
    Simple or basic SQL select.
        - Spark SQL
        - Dataframe API
    
    Selecting id, amount, date, description columns from food dataframe
    """
    spark_sql = spark.sql("select id, amount, date, description from foods_view")
    dataframe_api = food_df.select(
        col("id"), col("amount"), col("date"), col("description")
    )

    """
    Aggregating by summing the amount column and grouping by date
    """
    spark_sql = spark.sql("SELECT sum(amount), date FROM foods_view GROUP BY date")
    dataframe_api = food_df.groupby(col("date")).agg({"amount": "sum"})

    """
    Filtering by date
    """
    spark_sql = spark.sql("SELECT * FROM foods_view WHERE date >= '2023-08-13'")
    dataframe_api = food_df.filter(col("date") == "2023-08-13")

    """
    Ordering by id
    """
    spark_sql = spark.sql(
        "SELECT * FROM foods_view WHERE date >= '2023-08-13' ORDER BY id"
    )
    dataframe_api = food_df.orderBy("date")

    """
    Joining two dataframes, filtering and ordering  
    """
    sparl_sql = spark.sql(
        "SELECT * FROM foods_view "
        "JOIN people_view ON foods_view.people_id = people_view.id "
        "WHERE foods_view.date >= '2023-08-13' ORDER BY people_view.id"
    )
    dataframe_api = food_df.alias("j1").join(
        people_df.alias("j2"), col("j1.people_id") == col("j2.id"), "inner"
    )
