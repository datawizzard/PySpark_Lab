from pyspark.sql import SparkSession
from pyspark.sql.functions import col
if __name__ == '__main__':
    spark = SparkSession.builder.appName("AppName").master("local[*]").config("hive.metastore.uris", "thrift://localhost:9083/")\
            .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse/") \
            .enableHiveSupport().getOrCreate()
    spark.sql("use cohort_c9")
    df1= spark.sql("Select * from emp_bucke")
    df2= df1.dropna()
    df2.show()
    df3 = df2.filter(col("id") == "101")
    df3.show()
    df3.write.mode("overwrite").format("orc").saveAsTable("cohort_c9.anandtable")


