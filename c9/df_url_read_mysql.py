import urllib
from pyspark.sql import  SparkSession
from pyspark.sql.functions import explode
from urllib.request import urlopen
if __name__ == '__main__':
    spark = SparkSession.builder.appName("ApppName").master('local[*]').getOrCreate()
    url = urllib.request.urlopen(str("https://randomuser.me/api/0.8")).read().decode("UTF-8")
    # print(str(url))
    df = spark.read.json(spark.sparkContext.parallelize([url]))

    # df.show()
    df1 = df.withColumn("results",explode(df.results))\
        .select("nationality","results.user.cell","results.user.dob",
                "results.user.email","results.user.gender","results.user.location.city",
                "results.user.location.zip","results.user.location.street","results.user.md5",
                "results.user.name.first","results.user.name.last","results.user.name.title")
    df1.show()
    df1.printSchema()
    df1.write.format("jdbc").mode("overwrite") \
        .option("url", "jdbc:mysql://localhost:3306/retail_db?useSSL=False") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("user", "root") \
        .option("password", "Welcome@123") \
        .option("dbtable", "json_mgr").save()

