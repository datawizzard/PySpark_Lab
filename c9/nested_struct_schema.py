from pyspark.sql import SparkSession
from pyspark.sql.types import  StructType,StructField,IntegerType,StringType
if __name__ == '__main__':
    spark = SparkSession.builder.appName("creating dataframe").master("local[*]").getOrCreate()
    mydata = [(101,('Divya','Noname','Anand'),'Patna'),(102,('Lucy','Nothing','Singh'),'Gaya'),(103,('Jatin','Murg','Verma'),'BGR')]
    myschema = StructType([
        StructField("ID", StringType(),True),
        StructField("Name",StructType([
            StructField("Fname", StringType(), True),
            StructField("MiddleName", StringType(), True),
            StructField("LName", StringType(), True)]
        )),
        StructField("City", StringType(), True)
    ])

    df1= spark.createDataFrame(mydata,myschema)
    df1.show()
    df1.printSchema()

    setCols = df1.select("Name.Fname")
    setCols.show()
    
