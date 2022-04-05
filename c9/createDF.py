from pyspark.sql import SparkSession;
if __name__ == '__main__':
    spark = SparkSession.builder.appName("creating dataframe").master("local[*]").getOrCreate()
    mydata = [(101,'Saif'),(102,'Vishal'),(103,'Ravish')]
    myschema = ['Id','Name']
    mydf1 = spark.createDataFrame(mydata,myschema)
    mydf1.show()
    mydf1.printSchema()