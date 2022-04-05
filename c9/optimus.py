from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
from pyspark.sql.functions import *
if __name__ == '__main__':
    spark = SparkSession.builder.appName("NewAssign").master("local[*]").getOrCreate()
    data = [("Pune","Delhi"),("Delhi","Pune"),("Pune","Mumbai"),("Mumbai","Delhi"),("Jaipur","Bangalore"),("Delhi","Mumbai")]
    schema = ["SRC","DESC"]
    df = spark.createDataFrame(data,schema)
    df.show()
    df1 = df.withColumn("col1",when(df.SRC < df.DESC,df.SRC).otherwise(df.DESC))\
        .withColumn("col2",when(df.SRC > df.DESC,df.SRC).otherwise(df.DESC))
    df1.show()
    df2 = df1.select(df1.col1,df1.col2).distinct()
    df2.show()


