from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col

"""
Spark fundamentals & Setting up:
--------------------------------
    ✨ Overview and significance in big data processing
    ✨ Setting up a work environment
    ✨ RDD, Datasets & Dataframes
    ✨ DataFrame API vs. Spark SQL

    
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


class SparkFundamentals(SparkSession):
    def creating_dataset_dataframe(self):
        _dataframe = Row("id", "amount", "date", "description", "people_id")
        services = Row("id", "amount", "date", "description", "people_id")
        _dataset = Row("id", "amount", "date", "description", "people_id")

        food_df = self.createDataFrame(
            [
                _dataframe(121, 240.00, "2023-08-10", "carrot", 23),
                _dataframe(122, 140.00, "2023-08-11", "banana", 23),
                _dataframe(123, 40.00, "2023-08-12", "cinnamon", 22),
                _dataframe(124, 340.00, "2023-08-13", "potato", 21),
                _dataframe(125, 45.00, "2023-08-13", "sugar", 22),
                _dataframe(126, 200.00, "2023-08-14", "cucumber", 21),
            ]
        )

        service_df = self.createDataFrame(
            [
                services(321, 240.00, "2023-08-10", "3d printer", 21),
                services(322, 140.00, "2023-08-11", "cinema", 22),
                services(323, 40.00, "2023-08-12", "cinema", 23),
                services(324, 340.00, "2023-08-13", "swimming", 22),
                services(325, 45.00, "2023-08-13", "house cleaning", 21),
                services(326, 200.00, "2023-08-14", "delivery food", 21),
            ]
        )

        # people_df = self.createDataFrame(
        #     [
        #         people(21, "mary", "1990-07-10"),
        #         people(22, "kat", "2000-04-11"),
        #         people(23, "bianca", "1989-08-12"),
        #     ]
        # )

        output_list = [food_df, service_df]
        return output_list

    def spark_fundamentals(self):
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

        dataframe_list = self.creating_dataset_dataframe()
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
        spark_sql = self.sql("select id, amount, date, description from foods_view")
        dataframe_api = food_df.select(
            col("id"), col("amount"), col("date"), col("description")
        )

        """
        Aggregating by summing the amount column and grouping by date
        """
        spark_sql = self.sql("SELECT sum(amount), date FROM foods_view GROUP BY date")
        dataframe_api = food_df.groupby(col("date")).agg({"amount": "sum"})

        """
        Filtering by date
        """
        spark_sql = self.sql("SELECT * FROM foods_view WHERE date >= '2023-08-13'")
        dataframe_api = food_df.filter(col("date") == "2023-08-13")

        """
        Ordering by id
        """
        spark_sql = self.sql(
            "SELECT * FROM foods_view WHERE date >= '2023-08-13' ORDER BY id"
        )
        dataframe_api = food_df.orderBy("date")

        """
        Joining two dataframes, filtering and ordering  
        """
        sparl_sql = self.sql(
            "SELECT * FROM foods_view "
            "JOIN people_view ON foods_view.people_id = people_view.id "
            "WHERE foods_view.date >= '2023-08-13' ORDER BY people_view.id"
        )
        dataframe_api = food_df.alias("j1").join(
            people_df.alias("j2"), col("j1.people_id") == col("j2.id"), "inner"
        )
