from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
from pyspark.sql.functions import *
if __name__ == '__main__':
    spark = SparkSession.builder.appName("NewAssign").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    rdd = sc.textFile("/home/saif/LFS/cohort_c9/datasets/movies.csv")
    header = rdd.first()
    rdd1 = rdd.filter(lambda x : x!=header)
    rd1 = rdd1.map(lambda x : x.split(",")[1])
    rd2 = rdd1.map(lambda  x : x.split(",")[2])
    rd3 = rd2.map(lambda  x : x.split("|"))
    for x in zip(rd1.collect(),rd3.collect()):
         print(x)

    df = spark.read.format("csv").options(header=True,inferSchema=True).\
        load("/home/saif/LFS/cohort_c9/datasets/movies.csv")
    df1 = df.select(df.title.alias("movieTitle"),explode(split(df.genres,'\|')).alias("genres"))
    df1.show(truncate=False)