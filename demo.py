from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
if __name__ == '__main__':
    spark = SparkSession.builder.master('local[*]').getOrCreate()

    df = spark.read.csv("D:\hadoop\ALL_SQL_SCHEMA\GARAGE_EMPLOYEE.csv", header=True, inferSchema=True)

    df_count = df.agg(count('*'))
    df_count = df_count.collect()[0][0]//2
    df = df.withColumn('id', monotonically_increasing_id())

    df = df.filter(col('id') < df_count)
    df.show()