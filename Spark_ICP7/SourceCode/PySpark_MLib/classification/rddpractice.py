import os
os.environ["SPARK_HOME"] = "C:\\shankar\\spark-2.3.1-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="C:\\winutils"
os.environ["JAVA_HOME"] ="C:\\Program Files\\Java\\jre1.8.0_121"
from pyspark import SparkContext,SparkConf

conf=SparkConf.setAppName("Working with RDDs").setMaster("local")
spark=SparkContext(conf=conf)
