from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField,StringType,IntegerType,FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
if __name__ == '__main__':
    spark = SparkSession.builder.appName("NewAssign").master("local[*]").getOrCreate()
    socketDf = spark.readStream.format("socket").option("host","localhost").option("port","9999").load()
    mySchema = StructType([
        StructField("device_id", IntegerType(), True),
        StructField("device_name",StringType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("lat", IntegerType(), True),
        StructField("long", IntegerType(), True),
        StructField("scale", StringType(), True),
        StructField("temp", IntegerType(), True),
        StructField("timestamp", FloatType(), True),
        StructField("zipcode", IntegerType(), True)
    ])
    df1 = socketDf.select(explode(split(col("value"),"\\|")).alias("value"))
    df2 = df1.select(from_json(col("value"),mySchema).alias("MyCols")).select("MyCols.*")

    results = df2.writeStream.format("console").outputMode("append").option("truncate","false")\
        .option("checkpointLocation","/home/saif/Desktop/checkpoint/c8").start()
    results.awaitTermination()
    



