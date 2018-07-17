data = sc.textFile("/readmespark.md")
linesWithSpark = textFile.filter(line => line.contains("Spark"))
linesWithSpark.cache()
linesWithSpark.count()

from org.apache.spark.storage import StorageLevel
puntos = [1,5,8,6,30]
rdd1 = sc.parallelize(puntos)
result = rdd1.map(x => x * x)
result.persist(StorageLevel.DISK_ONLY)
result.collect()