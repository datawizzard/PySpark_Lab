from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
from pyspark.sql.functions import col
if __name__ == '__main__':
    spark = SparkSession.builder.appName("NewApp").master("local[*]").getOrCreate()
    data = [("Saif", "Sales", 3000),
            ("Saif", "Sales", 4600),
            ("Saif", "Sales", 4100),
            ("Kajal", "Finance", 3000),
            ("Neha", "Sales", 3000),
            ("Ram", "Finance", 3300),
            ("Shyam", "Finance", 3300),
            ("Aniket", "Finance", 3900),
            ("Shravan", "Marketing", 3000),
            ("Pramod", "Marketing", 2000),
            ("Vivek", "Sales", 4100)]
    myschema = StructType([
        StructField("Name", StringType(), True),
        StructField("Dept", StringType(), True),
        StructField("Salary", IntegerType(), True)
    ])
    df1= spark.createDataFrame(data,myschema)
    df1.show()
    # DropDuplicates drop the row based on columns.If two column are mentioned it will take composite of both the
    # column and find the distinct
    df2 = df1.dropDuplicates(["Dept","Salary"])
    df2.show()