data = sc.parallelize([('A',1),('b',2),('c',3)])
data2 =sc.parallelize([('A',4),('A',6),('b',7),('c',3),('c',8)])
result = data.join(data2)
print(result.collect())


rdd1 = sc.parallelize([(1,"jan",2016),(3,"nov",2014, (16,"feb",2014))])
rdd2 = sc.parallelize([(5,"dec",2014),(1,"jan",2016)])
comman = rdd1.intersection(rdd2)
comman.collect()


rdd1 = sc.parallelize([(1,"jan",2016),(3,"nov",2014),(16,"feb",2014),(3,"nov",2014)])
result = rdd1.distinct()
print(result.collect())


data = sc.parallelize([('k',5),('s',3),('s',4),('p',7),('p',5),('t',8),('k',6)],3)
group = data.groupByKey().collect()
for x in group:\
	print(x)

words = ["one","two","two","four","five","six","six","eight","nine","ten"]
data = sc.parallelize(words).map(lambda w : (w,1))
data2 = data.reduceByKey(lambda accum, n: accum + n)
data2.collect()


data = sc.parallelize([("maths",52), ("english",75), ("science",82), ("computer",65), ("maths",85)])
sorted = data.sortByKey()
sorted.collect()


rdd1 = sc.parallelize(["jan","feb","mar","april","may","jun"],3)
result = rdd1.coalesce(2)
result.collect()


def f(x): print(x)   

data = sc.parallelize([('k',5),('s',3),('s',4),('p',7),('p',5),('t',8),('k',6)],3)
group = data.groupByKey()
group.foreach(f)



