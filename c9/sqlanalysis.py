from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring
if __name__ == '__main__':
    spark = SparkSession.builder.appName("AppName").master("local[*]").config("hive.metastore.uris", "thrift://localhost:9083/")\
            .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse/") \
            .enableHiveSupport().getOrCreate()
    spark.sql("use adventureworks")
    df1= spark.sql("Select * from ext_cust")
    df2= df1.dropna()
    # df2.show()
    df3 = df2.select(df2.customerid,df2.territoryid,df2.accountnumber,df2.customertype,
        df2.modifieddate,df2._c5.getItem(0).alias("TotalPurchaseYTD"),df2._c6.getItem(0).alias("DateFirstPurchase"),
        df2._c7.getItem(0).alias("BirthDate"),df2._c8.getItem(0).alias("MaritalStatus"),df2._c9.getItem(0).alias("YearlyIncome"),
        df2._c10.getItem(0).alias("Gender"),df2._c11.getItem(0).alias("TotalChildren"),df2._c12.getItem(0).alias("NumberChildrenAtHome"),
        df2._c13.getItem(0).alias("Education"),df2._c14.getItem(0).alias("Occupation"),df2._c15.getItem(0).alias("HomeOwnerFlag"),
        df2._c16.getItem(0).alias("NumberCarsOwned"),df2._c17.getItem(0).alias("CommuteDistance"))
    # df3.show()
    # df3.printSchema()

    df4 = df3.select(df3.customerid,df3.territoryid,df3.accountnumber,df3.customertype,
        df3.modifieddate,df3.TotalPurchaseYTD.cast("float"), substring("DateFirstPurchase",1,10).alias('DateFirstPurchase').cast("date"),
                     substring("BirthDate",1,10).alias('BirthDate').cast("date"),df3.YearlyIncome,df3.Gender,df3.TotalChildren.cast("int"),
                     df3.NumberChildrenAtHome.cast("int"),df3.Education,df3.Occupation,df3.HomeOwnerFlag.cast("int"),
                     df3.NumberCarsOwned.cast("int"),df3.CommuteDistance)

    df4.show()
    # df4.printSchema()
    # df4.write.mode("overwrite").format("parquet").saveAsTable("adventureworks.sqlhive")

