val data = sc.parallelize(Array(('A',1),('b',2),('c',3)))
val data2 =sc.parallelize(Array(('A',4),('A',6),('b',7),('c',3),('c',8)))
val result = data.join(data2)
println(result.collect().mkString(","))


val data = sc.parallelize(Array(('k',5),('s',3),('s',4),('p',7),('p',5),('t',8),('k',6)),3)
val group = data.groupByKey().collect()
val twoRec = result.take(2)
twoRec.foreach(println)

val data = Array(1,23,100,-4,45)
val tops = sc.parallelize(data)
val tops2 = tops.top(3)
tops2.foreach(println)


val data = sc.textFile("/readme.md")
val result= data.map(line => (line,line.length)).countByValue()
result.foreach(println)


val rdd1 = sc.parallelize(List(20,32,45,62,8,5))
val sum = rdd1.reduce(_+_)
println(sum)


//Ejemplo adicional
val rdd1 = sc.parallelize(List(("maths", 80),("science", 90)))
val additionalMarks = ("extra", 4)
val sum = rdd1.fold(additionalMarks){ (acc, marks) => val add = acc._2 + marks._2


val employeeData = sc.parallelize(List(("Jack",1000.0),("Bob",2000.0),("Carl",7000.0)))
val dummyEmployee = ("dummy",0.0)
val maxSalaryEmployee = employeeData.fold(dummyEmployee)((acc,employee) => { 
if(acc._2 < employee._2) employee else acc})
println("employee with maximum salary is"+maxSalaryEmployee)


val data = sc.parallelize(Array(('k',5),('s',3),('s',4),('p',7),('p',5),('t',8),('k',6)),3)
val group = data.groupByKey().collect()
group.foreach(println)