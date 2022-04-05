from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder \
    .master("local[3]") \
    .appName("Read-Write Snowflake Table") \
    .config("spark.jars","file:///home/saif/LFS/cohort_c9/jars/snowflake-jdbc-3.13.4.jar, file:///home/saif/LFS/cohort_c9/jars/spark-snowflake_2.12-2.9.0-spark_3.1.jar") \
    .getOrCreate()

    sfDf = spark.read.format("net.snowflake.spark.snowflake") \
        .options(sfURL="https://bs14993.ap-south-1.aws.snowflakecomputing.com",
                 sfAccount="bs14993",
                 sfUser="DivyaAnand21",
                 sfPassword="Divya@123",
                 sfDatabase="SF_MYDB",
                 sfSchema="PUBLIC",
                 sfRole="ACCOUNTADMIN") \
        .option("dbtable", "emp") \
        .load()
    sfDf.show(truncate=False)
    sfDf.createOrReplaceTempView("emp")

    mgrEmpDf = spark.sql("""
            SELECT A.EMPNO AS EMP_EMPNO,
    		A.ENAME AS EMP_NAME,
          		B.EMPNO AS MGR_EMPNO,
          		B.ENAME AS MGR_NAME,
          		C.EMP_CNT AS MGR_TEAM_CNT
            FROM EMP A, EMP B,
    (SELECT A.MGR, COUNT(A.ENAME) EMP_CNT FROM EMP A, EMP B WHERE A.MGR=B.EMPNO GROUP BY A.MGR) C
            WHERE A.MGR=B.EMPNO
            AND B.EMPNO=C.MGR
            ORDER BY B.EMPNO
        """)
    # mgrEmpDf.show(truncate=False)

    sfOptions = {
        "sfURL": "bs14993.ap-south-1.aws.snowflakecomputing.com",
        "sfAccount": "bs14993",
        "sfUser": "DivyaAnand21",
        "sfPassword": "Divya@123",
        "sfDatabase": "sf_mydb",
        "sfSchema": "public",
        "sfWarehouse": "sf_myWh",
        "sfRole": "ACCOUNTADMIN"
    }

    mgrEmpDf.write \
        .format("snowflake") \
        .options(**sfOptions) \
        .option("dbtable", "emp_mgr") \
        .mode("append") \
        .options(header=True) \
        .save()

    print("**Data Written to Snowflake Successfuly**")