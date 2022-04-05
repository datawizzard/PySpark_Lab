from pyspark.sql import SparkSession
from pyspark.sql.types import  StructType,StructField,IntegerType,StringType
if __name__ == '__main__':
    spark = SparkSession.builder.appName("creating dataframe").master("local[*]").getOrCreate()
    mydata = [(101,'Saif'),(102,'Vishal'),(103,'Ravish')]
    myschema = StructType([
        StructField("ID",IntegerType(),True),
        StructField("Name", StringType(), True),
    ])
    df1 = spark.createDataFrame(mydata,myschema)
    df1.show()
    df1.printSchema()