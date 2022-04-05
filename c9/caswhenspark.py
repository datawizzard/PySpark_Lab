from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
from pyspark.sql.functions import col,when,expr
if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    data = [("Saif", "", "Shaikh", "36636", "M", 60000),
            ("Ram", "Shirali", "", "40288", "M", 70000),
            ("Aniket", "", "Mishra", "42114", "", 400000),
            ("Mitali", "Sahil", "Kashiv", "39192", "F", 500000),
            ("Nahid", "Alim", "Shaikh", "", "F", 0)]
    myschema = StructType([
        StructField("FName", StringType(), True),
        StructField("MName", StringType(), True),
        StructField("LName", StringType(), True),
        StructField("ZipCode", StringType(), True),
        StructField("Gender", StringType(), True),
        StructField("Salary", IntegerType(), True)
    ])

    df1 = spark.createDataFrame(data,myschema)
    df1.show()
    # Method I
    # df2 = df1.withColumn("GenderDesc",when(df1.Gender=='M',"Male").when(df1.Gender=='F',"Female").otherwise("NA"))
    # df2.show()
    # Method II
    df2 = df1.withColumn("GenderDesc",
                         expr("CASE WHEN Gender = 'M' THEN 'Male' " +
                              "WHEN Gender = 'F' THEN 'Female'" +
                              "ELSE 'NA' END"))
    df2.show()

