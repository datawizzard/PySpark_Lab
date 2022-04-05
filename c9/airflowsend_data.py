from pyspark.sql import SparkSession
from pyspark.sql.functions import col,split
import pyspark.sql.functions as f
if __name__ == '__main__':
    spark = SparkSession.builder.config("spark.jars","/home/saif/LFS/cohort_c9/jars/mysql-connector-java-8.0.28.jar")\
        .appName("TXNS").master("local[*]").getOrCreate()
    df = spark.read.format("csv").options(header=True).load("file:///home/saif/LFS/datasets/txns.csv")
    df1 = df.withColumn("txndate", f.to_date(col("txndate"), "MM-dd-yyyy"))
    df2 = df1.select("*",f.year(df1.txndate),f.month(df1.txndate),f.dayofmonth(df1.txndate))
    df3 = df2.filter(df2.category == 'Exercise & Fitness')
    df4 = df3.select("*",split(df3.category,"&").getItem(0).alias("New_Ex"),split(df3.category,"&").getItem(1).alias("New_Fit"))
    df4.show()
    df4.write.format("jdbc").mode("overwrite") \
        .option("url", "jdbc:mysql://localhost:3306/retail_db?useSSL=False") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("user", "root") \
        .option("password", "Welcome@123") \
        .option("dbtable", "airflowtbl").save()
