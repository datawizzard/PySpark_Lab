from pyspark.sql import SparkSession
if __name__ == '__main__':
    spark = SparkSession.builder.config("spark.jars","/home/saif/LFS/cohort_c9/jars/spark-xml_2.12-0.5.0.jar").appName("Read & Write ").master("local[*]").getOrCreate()
    df = spark.read \
        .format("com.databricks.spark.xml") \
        .option("rootTag", "catalog") \
        .option("rowTag", "book") \
        .load("/home/saif/LFS/cohort_c9/datasets/book_details.xml")
    df.show()
    df.printSchema()
    df.write.format("xml").mode("overwrite").save("hdfs://localhost:9000/user/saif/HFS/Input/")

