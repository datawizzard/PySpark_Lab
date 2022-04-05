from pyspark.sql import SparkSession
if __name__ == '__main__':
    spark =  SparkSession.builder.appName("creating dataframe").master("local[3]").config("spark.sql.shuffle.partitions","2").getOrCreate()
    df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("file:///home/saif/LFS/cohort_c9/datasets/medicalData.csv")
    partition_df = df.repartition(2)
    filter_df = partition_df.where("Age<40")
    select_df = filter_df.select("Age","Gender","Country","State")
    group_df = select_df.groupBy("Country")
    count_df = group_df.count()
    count_df.collect()
    input("Enter to Quit")