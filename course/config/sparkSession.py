from pyspark.sql import SparkSession
from course.project import create_dataframe, basic_operations, working_with_schemas

__spark = SparkSession.builder.master("local").appName("testing").getOrCreate()

if __name__ == "__main__":
    df_list = create_dataframe(__spark)
    basic_operations(df_list, __spark)
