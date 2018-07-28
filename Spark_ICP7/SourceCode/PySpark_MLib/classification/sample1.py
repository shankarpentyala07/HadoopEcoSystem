import os
os.environ["SPARK_HOME"] = "C:\\shankar\\spark-2.3.1-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="C:\\winutils"
os.environ["JAVA_HOME"] ="C:\\Program Files\\Java\\jre1.8.0_121"
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("exam practice").getOrCreate()
dataset = spark.read.text("C:\\PySpark_MLib\\data\\classification\\Absenteeism_at_work.txt")
print(dataset.select(size(split(dataset.value,"\t")).name("num of words")).agg(max(("num of words"))).collect())
x= dataset.select(explode(split(dataset.value,"\t")).name("words")).groupBy("words").count().collect()
print(x.value)