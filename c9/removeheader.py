from pyspark import SparkContext
from pyspark import SparkConf

if __name__ == '__main__':
    sparkConf = SparkConf()
    sc = SparkContext(conf=sparkConf)
    readLine = sc.textFile("file:///home/saif/LFS/cohort_c9/datasets/emp_new.txt")
    hdrremove = readLine.first()
    removeHdr = readLine.filter(lambda x: x!=hdrremove)
    print(removeHdr.collect())