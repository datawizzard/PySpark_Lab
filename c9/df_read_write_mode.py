from pyspark.sql import SparkSession
if __name__ == '__main__':
    spark = SparkSession.builder.appName("Read & Write ").master("local[*]").getOrCreate()
    mydf = spark.read.format("csv").options(header=True,inferSchema=True,delimeter=',').load("file:///home/saif/LFS/cohort_c9/datasets/medicalData.csv")

    mydf.write.format("csv").mode("append").save("hdfs://localhost:9000/user/saif/HFS/Output/read_write")
    print("Data written succesfully")
    mydf.show()
    mydf.printSchema()

