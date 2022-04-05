from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

from pyspark.sql.functions import col, array_contains,avg,max,min,sum,lit,abs
if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    data = [('Saif', '', 'Shaikh', '1991-04-01', 'M', 3000),
            ('Ram', 'Sachin', '', '2000-05-19', 'M', 4000),
            ('Aniket', '', 'Mishra', '1978-09-05', 'M', 4000),
            ('Mitali', 'Sahil', 'Kashiv', '1967-12-01', 'F', 4000),
            ('Nahid', 'Alim', 'Shaikh', '1980-02-17', 'F', -1)]
    myschema = StructType([
        StructField("Name", StringType(), True),
        StructField("Mname", StringType(), True),
        StructField("Lname", StringType(), True),
        StructField("Date", StringType(), True),
        StructField("Gender", StringType(), True),
        StructField("Hike", IntegerType(), True)
    ])
    df1= spark.createDataFrame(data,myschema)
    # Add a column
    df2 = df1.withColumn("location",lit("Patna"))
    # df2.show()
    # df2.printSchema()
    # # Change  the datatype of the column
    # df3 = df2.withColumn("Hike",col("Hike").cast("Long"))
    # df3.printSchema()
    # Doing some computation and save it new column
    df4 = df2.withColumn("Increase_sal",abs(col("Hike"))*100).withColumn("Country",lit("India"))
    df4.show()
    df5 = df2.withColumn("Increase_sal",abs(col("Hike"))*100).withColumn("Country",lit("India")).withColumnRenamed("Increase_sal","Revised_sal").drop("Hike")
    df5.show()
