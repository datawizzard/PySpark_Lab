from pyspark.sql import SparkSession
if __name__ == '__main__':
    spark = SparkSession.builder.appName("Avro").config("spark.jars","/home/saif/LFS/cohort_c9/jars/spark-avro_2.12-3.0.1.jar").master("local[*]").getOrCreate()
    df = spark.read.format("avro").load("/home/saif/LFS/cohort_c9/datasets/users.avro")
    df.show()
