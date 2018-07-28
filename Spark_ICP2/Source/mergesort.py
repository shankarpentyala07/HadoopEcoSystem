import os
os.environ["SPARK_HOME"] = "C:\\shankar\\spark-2.3.1-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="C:\\winutils"
from pyspark import SparkContext

def merge(left,right):
    i = 0
    j = 0

    leftlen = len(left)
    rightlen = len(right)
    leftright = []
    while (i < leftlen and j< rightlen ):
        if (left[i] < right[j]):
            leftright.append(left[i])
            i = i + 1
        else:
            leftright.append(right[j])
            j = j + 1
    while (i < leftlen):
        leftright.append(left[i])
        i = i + 1

    while (j < rightlen):
        leftright.append(right[j])
        j = j + 1
    return leftright





def mergesort(arr,low,high):
    if (low == high):
        return [arr[low]]
    elif (low +1 == high):
         if(arr[low] > arr[high]):
             return [arr[high],arr[low]]
         else:
             return[arr[low],arr[high]]
    mid = int((low + high) / 2)
    left = mergesort(arr,low,mid)
    right = mergesort(arr,mid+1,high)
    return merge(left,right)




if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    rdd = sc.parallelize(['7','1','3','2','6','5'])
    rddsort = rdd.zipWithIndex().sortByKey().map(lambda x:x[0]).collect()
    print(rddsort)

   # print(inparray.collect())
    #length = len(inparray)
    #print(mergesort(inparray,0,length-1))






