import os
os.environ["SPARK_HOME"] = "C:\\shankar\\spark-2.3.1-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="C:\\winutils"
os.environ["JAVA_HOME"] ="C:\\Program Files\\Java\\jre1.8.0_121"

from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint


from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint

from pyspark import SparkContext
global count
count = 0

def filterfunct(self,k,v):
    if k == v:
        self.count= self.count + 1

def parseLine(line):
    parts = line.split(',')
    label = float(parts[0])
    features = Vectors.dense([float(x) for x in parts[1].split(',')])
    return LabeledPoint(label, features)


sc = SparkContext(appName="PythonNaiveBayesExample")
data = sc.textFile('C:\\PySpark_MLib\\data\\classification\\Immunotherapy.csv').map(parseLine)

# Split data aproximately into training (60%) and test (40%)
training, test = data.randomSplit([0.6, 0.4], seed=0)

# Train a naive Bayes model.
model = NaiveBayes.train(training, 1.0)

# Make prediction and test accuracy.
predictionAndLabel = test.map(lambda p: (model.predict(p.features), p.label))

predictionAndLabel.filter(filterfunct)

# Save and load model
model.save(sc, "target/tmp/myNaiveBayesModel")
sameModel = NaiveBayesModel.load(sc, "target/tmp/myNaiveBayesModel")
