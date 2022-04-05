from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
from pyspark.sql.functions import *
if __name__ == '__main__':
    spark = SparkSession.builder.appName("NewAssign").master("local[*]")\
        .config("hive.metastore.uris", "thrift://localhost:9083/")\
            .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse/") \
            .enableHiveSupport().getOrCreate()
    data = [(1,'buy',100),(2,"buy",20),(1,"sell",150),(2,"buy",30),(2,"sell",50),(3,"buy", 300)]
    schema = ["id","flag","profit"]
    df = spark.createDataFrame(data,schema)
    df.show()
    df1 = df.groupBy("id")\
        .agg(sum(expr("case when flag='buy' then -profit else profit end"))\
             .alias("Total_Profit")).orderBy("id")
    df1.show()
    df1.write.mode("overwrite").saveAsTable("cohort_c9.impetustable")
