from pyspark import SparkConf,SparkContext
if __name__ == '__main__':
    sparCon = SparkConf()
    sc = SparkContext(conf=sparCon)
    textf = sc.textFile("/home/saif/LFS/cohort_c9/datasets/ratings.csv")
    rdds = textf.map(lambda x : (x.split(",")[2]))
    new_rdds = rdds.map(lambda x : (x,1))
    res = new_rdds.reduceByKey(lambda x,y:x+y).sortBy(lambda x : x[0],ascending=False)
    count = 0
    for i in res.collect():
        if count == 0:
            count += 1
            continue
        print(i)
