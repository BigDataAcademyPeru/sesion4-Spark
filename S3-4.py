from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("sparkcontext").setMaster("local")
sc = SparkContext(conf=conf)
data = sc.textFile("/readme.md")
data.take(2)
