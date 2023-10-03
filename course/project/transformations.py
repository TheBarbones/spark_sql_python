import pyspark.sql.functions as F
import course.sparkSession as S


def testing(df):
    return df.withColumn("greeting", F.lit("hello!"))


source_data = [("jose", 1), ("li", 2)]
data = S.spark.createDataFrame(source_data)
data.show()
