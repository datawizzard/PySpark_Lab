from pyspark.sql import SparkSession
if __name__ == '__main__':
    spark = SparkSession.builder.appName("Read & Write ").master("local[*]").getOrCreate()
    mydf = spark.read.format("csv").options(header=True,inferSchema=True,delimeter=',').load("file:///home/saif/LFS/cohort_c9/datasets/medicalData.csv")
    mydf.show()
    mydf.printSchema()

