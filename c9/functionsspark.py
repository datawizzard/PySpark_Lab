from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
from pyspark.sql.functions import concat,concat_ws,date_format,unix_timestamp,from_unixtime,date_add,date_sub
if __name__ == '__main__':
    spark = SparkSession.builder.appName("App Name").master("local[*]").getOrCreate()
    data = [("Saif", "M", "Shaikh", "2018", "M", 3000),
            ("Ram", "S", "Shirali", "2010", "M", 4000),
            ("Mitali", "S", "Kashiv", "2010", "M", 4000),
            ("Anup", "B", "Garje", "2005", "F", 4000),
            ("Sagar", "S", "Shinde", "2010", "", -1)]
    myschema = StructType([
        StructField("Name", StringType(), True),
        StructField("Type", StringType(), True),
        StructField("LName", StringType(), True),
        StructField("Date", StringType(), True),
        StructField("Gender", StringType(), True),
        StructField("Salary", IntegerType(), True)
    ])
    df = spark.createDataFrame(data,myschema)
    df.show()
    # df1 = df.select(concat(df.Name,df.LName).alias("Full_Name"))
    # df1.show()
    df2 = df.select(concat_ws('-',df.Name,df.LName).alias("Full_Name_del"))
    df2.show()

    dt_Df = spark.createDataFrame([('1997-03-21','1997-02-21')],['start_date','end_date'])
    dt_Df.show()
    # dt_Df.printSchema()
    dt_2 = dt_Df.withColumn('start_date',dt_Df.start_date.cast('date')).withColumn('end_date',dt_Df.end_date.cast('date'))
    # dt_2.printSchema()
    df2 = dt_2.select(date_format(dt_2.start_date,'dd-MM-yyyy').alias("New_Date"))
    df2.show()
    df3 = df2.select(from_unixtime(unix_timestamp(df2.New_Date,'dd-MM-yyyy'), 'dd-MM-yyyy').alias("test"))
    df4 = df3.withColumn("test",df3.test.cast('date'))
    df4.show()
    df4.printSchema()
    df5 = dt_2.select(date_add(dt_2.start_date,4).alias("Add_Date"))
    df5.show()

