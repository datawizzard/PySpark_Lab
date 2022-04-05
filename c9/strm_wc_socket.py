from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import explode,split
if __name__ == '__main__':
    spark = SparkSession.builder.appName("NewAssign").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    socketdf = spark.readStream.format("socket")\
    .option("host", "localhost")\
    .option("port", "9999")\
    .load()

    df1 = socketdf.select(explode(split("value"," ")).alias("word"))
    wc = df1.groupBy("word").count()
    res= wc.writeStream.format("console").\
        outputMode("update").option("truncate","False").option("checkpoint","/home/saif/Desktop/dir2")\
        .start()
    res.awaitTermination()
