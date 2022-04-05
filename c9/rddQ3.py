from pyspark.sql import SparkSession
from pyspark.sql.functions import sum,count
if __name__ == '__main__':
    spark = SparkSession.builder.appName("NewAssign").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    #RDD
    textf = sc.textFile("/home/saif/LFS/cohort_c9/datasets/ratings.csv")
    header = textf.first()
    rddf = textf.filter(lambda x: x != header)
    splitRDD = rddf.map(lambda x: (int(x.split(",")[1]), float(x.split(",")[2])))
    resultRdd = splitRDD.aggregateByKey((0.0, 0), lambda x, y: (x[0] + y, x[1] + 1),
                                        lambda x, y: (x[0] + y[0], x[1] + y[1]))
    res = resultRdd.map(lambda x: (x[0], x[1][0] / x[1][1]))
    for x in res.take(5):
        print(x)

    # for x in textf.take(5):
    #     print(x)
    #DF
    df = spark.read.format("csv").options(header=True, inferSchema=True).load(
         '/home/saif/LFS/cohort_c9/datasets/ratings.csv')
    df1 = df.groupBy(df.movieId).agg(sum(df.rating).alias("sums"),count(df.rating).alias("counts"))
    df2 = df1.select(df.movieId,(df1.sums/df1.counts).alias("AVG"))
    df2.show(5)
    df.write.format("csv").mode("overwrite").save("hdfs://localhost:9000/user/saif/HFS/Input/python")
