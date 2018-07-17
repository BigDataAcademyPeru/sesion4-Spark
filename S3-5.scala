
val data = sc.textFile("/readmespark.md")
val linesWithSpark = data.filter(line => line.contains("Spark"))
linesWithSpark.cache()
linesWithSpark.count()

import org.apache.spark.storage.StorageLevel
val puntos = Array(1,5,8,6,30)
val rdd1 = sc.parallelize(puntos)
val result = rdd1.map(x => x * x)
result.persist(StorageLevel.DISK_ONLY)
println(result.count())
println(result.collect().mkString(","))

