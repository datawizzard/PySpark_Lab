from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,dense_rank,rank,explode
from pyspark.sql.functions import collect_list,collect_set
if __name__ == '__main__':
    spark = SparkSession.builder.appName("MyApp").master("local[*]").getOrCreate()
    arrayData = [('Saif', ['Java', 'Scala'], {'hair': 'black', 'eye': 'brown'}),
                 ('Mitali', ['Spark', 'Java', None], {'hair': 'brown', 'eye': None}),
                 ('Ram', ['CSharp', ''], {'hair': 'red', 'eye': ''}),
                 ('Wilma', None, None),
                 ('Jatin', ['1', '2'], {})]
    schema = ['Name','Language','Properties']
    df = spark.createDataFrame(arrayData,schema)
    df1 = df.withColumn("test",explode(df.Language))
    df2= df.select("*",explode(df.Properties)).select("*",explode(df.Language))
    df2.show()