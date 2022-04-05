from pyspark import  *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("pyspark-notebook2").master("local[*]")\
    .config("spark.mongodb.input.uri","mongodb://localhost:27017,mongo2:27017,mongo3:27017/assign.collect")\
    .config("spark.mongodb.output.uri","mongodb://localhost:27017,mongo2:27017,mongo3:27017/assign.collect")\
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0")\
    .getOrCreate()

df = spark.read.format("mongo").option("uri","mongodb://127.0.0.1:27017/assign.collect").load()
df.printSchema()
df.show()
df.collect()
df.write.format("mongo").option('uri', 'mongodb://127.0.0.1:27017/anand_db.new')\
    .mode("append") \
    .save()