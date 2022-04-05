from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import col, array_contains,lit

if __name__ == '__main__':
    sparkConfObj = SparkConf().setAppName("filter as").setMaster("local[*]")
    sc = SparkContext(conf=sparkConfObj)
    spark = SparkSession.builder.getOrCreate()
    arrayData = [
        (("Saif", "", "Shaikh"), ["English", "Science", "Maths"], "HYD", "M"),
        (("Ram", "Sachin", ""), ["Spark", "English", "Maths"], "BLR", "F"),
        (("Aniket", "", "Mishra"), ["Civics", "History"], "HYD", "F"),
        (("Mitali", "Sahil", "Kashiv"), ["Civics", "History"], "BLR", "M"),
        (("Zaid", "Riyaz", "Shaikh"), ["Civics", "History"], "BLR", "M"),
        (("Sufi", "Alim", "Shaikh"), ["Hindi", "History"], "HYD", "M")]

    myschema = StructType([
        StructField("Name", StructType([
            StructField("Fname",StringType(),True),StructField("Mname",StringType(),True),StructField("Lname",StringType(),True)
        ]), True),
        StructField("Subject", ArrayType(StringType()),True),
        StructField("Place",StringType(),True),
        StructField("gender", StringType(), True)
        ])
    df = spark.createDataFrame(arrayData,myschema)
    df.show(truncate=False)
    print(myschema)
    df2 = df.filter((df.Place == 'HYD') & (df.gender == 'F')).show()
    df3 = df.filter(array_contains(df.Subject,lit("Spark")))
    df3.show(truncate = False)
