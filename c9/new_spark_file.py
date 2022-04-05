from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

from pyspark.sql.functions import col, array_contains,avg,max,min,sum
if __name__ == '__main__':
    spark = SparkSession.builder.appName("creating dataframe").master("local[*]").getOrCreate()
    simpleData1 = [("Saif", "Sales", "MUM", 90000, 34, 10000),
                   ("Aniket", "Sales", "MUM", 86000, 56, 20000),
                   ("Ram", "Sales", "PUN", 81000, 30, 23000),
                   ("Mitali", "Finance", "PUN", 90000, 24, 23000)]
    simpleData2 = [("Saif", "Sales", "MUM", 90000, 34, 10000),
                   ("Mitali", "Finance", "PUN", 90000, 24, 23000),
                   ("Sufiyan", "Finance", "MUM", 79000, 53, 15000),
                   ("Alim", "Marketing", "PUN", 80000, 25, 18000),
                   ("Amit", "Marketing", "MUM", 91000, 50, 21000)]
    myschema = StructType([
        StructField("Name",StringType(),True),
        StructField("Dept",StringType(),True),
        StructField("Place", StringType(), True),
        StructField("Sal", IntegerType(), True),
        StructField("Age", IntegerType(), True),
        StructField("Comm", IntegerType(), True)
    ])

    df1 = spark.createDataFrame(simpleData1, myschema)
    df2 = spark.createDataFrame(simpleData2, myschema)
    df1.show(truncate=False)
    df2.show(truncate=False)
    # Combine two tables and remove the duplicates in the table using distinct
    df3 = df1.union(df2).distinct()
    df3.show()