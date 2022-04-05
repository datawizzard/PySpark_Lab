from pyspark.sql import SparkSession
if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.format("csv").options(header=True,inferSchema=True).load('/home/saif/LFS/cohort_c9/datasets/ratings.csv')
    # df.show(truncate =False)
    df1 = df.groupBy(df.rating).count()
    # df1.show()
    df2 = df1.orderBy(df1.rating.desc())
    df2.show()