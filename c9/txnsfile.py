from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark.sql.functions as f
if __name__ == '__main__':
    spark = SparkSession.builder.appName("TXNS").master("local[*]").getOrCreate()
    df = spark.read.format("csv").options(header=True).load("/home/saif/LFS/datasets/txns.csv")
    df1 = df.withColumn("txndate", f.to_date(col("txndate"), "MM-dd-yyyy"))
    df1.printSchema()
    df1.show()
    df2 = df1.select("*",f.year(df1.txndate),f.month(df1.txndate),f.dayofmonth(df1.txndate))
    df2.show()
