from pyspark import SparkConf,SparkContext,SQLContext
sparkConf = SparkConf()
sc = SparkContext(conf=sparkConf)
sqlConn = SQLContext(sc)
data = [(101,'Saif'),(102,'Vishal'),(103,'Ravish')]
rdd = sc.parallelize(data)
df1 = rdd.toDF(['Id','Name'])
df1.show()
df2 = sqlConn.createDataFrame(rdd)
df2.show()