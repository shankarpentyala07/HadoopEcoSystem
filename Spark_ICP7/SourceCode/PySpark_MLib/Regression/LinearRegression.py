import os
os.environ["SPARK_HOME"] = "C:\\shankar\\spark-2.3.1-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="C:\\winutils"
os.environ["JAVA_HOME"] ="C:\\Program Files\\Java\\jre1.8.0_121"

from pyspark.ml.regression import LinearRegression, SparkSession

# Load training data
#from pyspark.python.pyspark.shell import spark

spark = SparkSession.builder.appName("Lesson7").getOrCreate()

training = spark.read.format("libsvm").load("C:\\PySpark_MLib\\data\Regression\\sample_linear_regression_data.txt")

lr = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

# Fit the model
lrModel = lr.fit(training)

# Print the coefficients and intercept for linear regression
print("Coefficients: %s" % str(lrModel.coefficients))
print("Intercept: %s" % str(lrModel.intercept))

# Summarize the model over the training set and print out some metrics
trainingSummary = lrModel.summary
print("numIterations: %d" % trainingSummary.totalIterations)
print("objectiveHistory: %s" % str(trainingSummary.objectiveHistory))
trainingSummary.residuals.show()
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)