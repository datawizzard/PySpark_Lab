from pyspark import SparkContext
from pyspark import SparkConf

if __name__ == '__main__':
    sparkConf = SparkConf()
    sc = SparkContext(conf=sparkConf)

    readLine = sc.textFile("file:///home/saif/LFS/cohort_c9/datasets/word_count.txt").flatMap(
        lambda line: line.split(","))
    mapped = readLine.map(lambda x: (x, 1))
    result = mapped.reduceByKey(lambda a, b: a + b)
    print(result.collect())
    result.saveAsTextFile("hdfs://localhost:9000/user/saif/HFS/Output/wc_spark_pyc")