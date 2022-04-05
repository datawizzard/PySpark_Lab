from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

from pyspark.sql.functions import col, array_contains,avg,max,min,sum
if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    orderingData = [("Saif", "Sales", "HYD", 90000, 34, 10000),
                  ("Ram", "Sales", "HYD", 86000, 56, 20000),
                  ("Aniket", "Sales", "MUM", 81000, 30, 23000),
                  ("Saima", "Finance", "MUM", 90000, 24, 23000),
                  ("Sufiyan", "Finance", "MUM", 99000, 40, 24000),
                  ("Alim", "Finance", "HYD", 83000, 36, 19000),
                  ("Mitali", "Finance", "HYD", 79000, 53, 15000),
                  ("Neha", "Marketing", "MUM", 80000, 25, 18000),
                  ("Kajal", "Marketing", "HYD", 91000, 50, 21000)]

    myschema = StructType([
    StructField("Name", StringType(),True),
    StructField("Designation",StringType(),True),
    StructField("Place",StringType(),True),
    StructField("Sal", IntegerType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Hike", IntegerType(), True)
    ])
    df1 = spark.createDataFrame(orderingData,myschema)
    df2 = df1.orderBy("Age","Hike",ascending=False).show()
    ##order column one in ascending and other in descending
    df3 = df1.orderBy(df1.Age,df1.Hike.desc()).show()
    ##Average sal based on each department
    df4 = df1.groupBy("Designation").agg(avg("Sal").alias("Avg")).show()

    df5 = df1.groupBy("Designation").agg(max("Sal").alias("Max Sal"),min("Sal").alias("Min Sal"),sum("Sal").alias("Sum_Sal"))
    df5.show()
    df6 = df5.filter(df5.Sum_Sal > 171000)
    df6.show()


