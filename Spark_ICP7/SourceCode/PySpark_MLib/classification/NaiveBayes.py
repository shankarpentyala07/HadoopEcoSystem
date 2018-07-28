import os
os.environ["SPARK_HOME"] = "C:\\shankar\\spark-2.3.1-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="C:\\winutils"
os.environ["JAVA_HOME"] ="C:\\Program Files\\Java\\jre1.8.0_121"
from pyspark.ml.classification import NaiveBayes, SparkSession
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.util import MLUtils
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors
from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint

def parseLine(line):
    parts = line.split(',')
    label = float(parts[7])
    features = Vectors.dense([float(parts[0])])
    return LabeledPoint(label, features)

# Load training data
#spark = SparkSession.builder.appName("Lesson7").getOrCreate()
sc = SparkContext(appName="PythonNaiveBayesExample")
data = sc.textFile("C:\\PySpark_MLib\\data\\classification\\Immunotherapy.csv").map(parseLine)


#data = sc.read("C:\\PySpark_MLib\\data\\classification\\Immunotherapy.csv")

# Split the data into train and test
splits = data.randomSplit([0.6, 0.4], 0)
train = splits[0]
test = splits[1]

# create the trainer and set its parameters
nb = NaiveBayes(smoothing=1.0, modelType="multinomial")

#pipeline = Pipeline().setStages(Array(assembler,lr))
# train the model
model = nb.fit(train)
model = NaiveBayes.train(train, 1.0)

# select example rows to display.
predictions = model.transform(test)
predictions.show()

# compute accuracy on the test set
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
 #                                             metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test set accuracy = " + str(accuracy))