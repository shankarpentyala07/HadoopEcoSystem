import os
os.environ["SPARK_HOME"] = "C:\\shankar\\spark-2.3.1-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="C:\\winutils"

from pyspark import SparkContext

def friendmap(value):

    value = value.split(" ")
    user = value[0]
    friends = value[1]
    keys = []

    for friend in friends:
        keys.append((''.join(sorted(user+friend)), friends.replace(friend, "")))

    return keys


def friendreduce(key, value):
    reducer = ''
    for friend in key:
        if friend in value:
            reducer += friend
    return reducer


if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    Lines = sc.textFile("C:\\Users\\shankar pentyala\\PycharmProjects\\ICP2\\lab.csv", 1)
    Line = Lines.flatMap(friendmap)
    Commonfriends = Line.reduceByKey(friendreduce)
    Commonfriends.coalesce(1).saveAsTextFile("CommonFriendsOutput")
    sc.stop()

