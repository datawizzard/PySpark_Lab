from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,dense_rank,rank,explode
from pyspark.sql.functions import collect_list,collect_set
if __name__ == '__main__':
    spark = SparkSession.builder.appName("MyApp").master("local[*]").getOrCreate()
    emp = [(1, "Saif", -1, "2018", "10", "M", 3000),
           (2, "Ram", 1, "2010", "20", "M", 4000),
           (3, "Aniket", 1, "2010", "10", "M", 1000),
           (4, "Mitali", 2, "2005", "10", "F", 2000),
           (5, "Nahid", 2, "2010", "40", "", -1),
           (6, "Sufiyan", 2, "2010", "50", "", -1)]
    dept = [("Finance", 10),
            ("Marketing", 20),
            ("Sales", 30),
            ("IT", 40)]
    schema = ['id','Name','mgdId','dates','Age','Gender','Sal']
    myschema = ['Dept','Age']
    df = spark.createDataFrame(emp,schema)
    df1 = spark.createDataFrame(dept,myschema)
    df.show()
    df1.show()

    ## INNER JOIN
    # ij = df.join(df1,df.Age==df1.Age,"inner")
    # ij.show()

    ## LEFT OUTER JOIN
    loj = df.join(df1, df.Age == df1.Age, "leftouter")
    loj.show()

    ## Right OUTER JOIN
    roj = df.join(df1,df.Age == df1.Age,"rightouter")
    roj.show()

    ## Full Outer Join
    foj = df.join(df1, df.Age == df1.Age, "fullouter")
    foj.show()

    ## Cross Join
    crossj = df.crossJoin(df1)
    crossj.show()

    ## Left Semi Join : Its similar to inner join but it will show data from left side of the table.
    lsj = df.join(df1,df.Age == df1.Age,"leftsemi")
    lsj.show()

    ## Left Anti : It will display all non matching data from left table
    lsj = df.join(df1, df.Age == df1.Age, "leftanti")
    lsj.show()


    emptbl = df.createTempView("tempemptbl")
    spark.sql("select * from tempemptbl").show()

