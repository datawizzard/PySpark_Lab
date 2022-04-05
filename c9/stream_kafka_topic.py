from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType,DoubleType,LongType
from pyspark.sql.functions import col,explode,split,from_json,expr
if __name__ == '__main__':
    spark = SparkSession.builder.appName("NewAssign").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    socketDf = spark.readStream.format("socket").option("host", "localhost").option("port", "9999").load()
    kafkadf = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers","localhost:9092")\
    .option("subscribe","myTopic")\
    .option("startingOffsets","earliest")\
    .load()
    schema = StructType([
        StructField("InvoiceNumber", StringType()),
        StructField("CreatedTime", LongType()),
        StructField("StoreID", StringType()),
        StructField("PosID", StringType()),
        StructField("CashierID", StringType()),
        StructField("CustomerType", StringType()),
        StructField("CustomerCardNo", StringType()),
        StructField("TotalAmount", DoubleType()),
        StructField("NumberOfItems", IntegerType()),
        StructField("PaymentMethod", StringType()),
        StructField("CGST", DoubleType()),
        StructField("SGST", DoubleType()),
        StructField("CESS", DoubleType()),
        StructField("DeliveryType", StringType()),
        StructField("DeliveryAddress", StructType([
        StructField("AddressLine", StringType()),
        StructField("City", StringType()),
        StructField("State", StringType()),
        StructField("PinCode", StringType()),
        StructField("ContactNumber", StringType())
        ])),
        StructField("InvoiceLineItems", ArrayType(StructType([
            StructField("ItemCode", StringType()),
            StructField("ItemDescription", StringType()),
            StructField("ItemPrice", DoubleType()),
            StructField("ItemQty", IntegerType()),
            StructField("TotalValue", DoubleType())
        ]))),
    ])
    df1 = kafkadf.select(from_json(col("value").cast("string"),schema).alias("value"))
    df1.printSchema()
    df2= df1.select(df1.value.InvoiceNumber,df1.value.StoreId,df1.value.CustomerType,
            df1.value.DeliveryAddress.AddressLine,explode(df1.value.InvoiceLineItems).alias("InvoiceLineItems"))
    df3 = df2.withColumn("ItemCode",expr("InvoiceLineItems.ItemCode"))
    results = df3.writeStream.format("console").outputMode("append").option("truncate", "false") \
        .option("checkpointLocation", "/home/saif/Desktop/checkpoint/c14").start()
    results.awaitTermination()