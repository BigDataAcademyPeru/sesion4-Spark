data = sc.parallelize([('A',1),('b',2),('c',3)])
data2 =sc.parallelize([('A',4),('A',6),('b',7),('c',3),('c',8)])
result = data.join(data2)
result.collect()


data = sc.parallelize([('k',5),('s',3),('s',4),('p',7),('p',5),('t',8),('k',6)],3)
group = data.groupByKey()
group.take(2)


data = [1,23,100,-4,45]
tops = sc.parallelize(data)
tops.top(3)


data = [1,23,100,-4,45,45,45,100]
result= sc.parallelize(data)
result.countByValue()


rdd1 = sc.parallelize([20,32,45,62,8,5])
suma = rdd1.reduce(lambda x,y: x+y)
print(suma)