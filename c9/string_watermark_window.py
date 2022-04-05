from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
from pyspark.sql.functions import sum,from_json,col,to_timestamp,expr,window
if __name__ == '__main__':
    spark = SparkSession.builder.appName("NewAssign").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    mySchema = StructType([
        StructField("CreatedTime",StringType(),True),
        StructField("Type",StringType(),True),
        StructField("Amount",StringType(),True),
        StructField("BrokerCode",StringType(),True)
    ])
    kafkadf = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "water") \
        .option("startingOffsets", "earliest") \
        .load()
    df1 = kafkadf.select(from_json(col("value").cast("string"),mySchema).alias("value"))
    df2 = df1.select("value.*")\
    .withColumn("CreatedTime",to_timestamp("CreatedTime","yyy-MM-dd HH:mm:ss"))\
    .withColumn("Buy",expr("case when Type == 'BUY' then Amount else 0 end"))\
    .withColumn("Sell",expr("case when Type == 'SELL' then Amount else 0 end"))
    df3 = df2.withWatermark("CreatedTime","30 minute").groupBy(window(col("CreatedTime"),"15 minute"))\
    .agg(sum("BUY").alias("Total_Buy"),

         sum("SELL").alias("Total_Sell"))
    df4 = df3.select("window.start","window.end","Total_Buy","Total_Sell")
    results = df3.writeStream.format("console").outputMode("complete").option("truncate", "false") \
        .option("checkpointLocation", "/home/saif/Desktop/checkpoint/c12").start()
    results.awaitTermination()