df = spark.read.csv("readme.md")
df
df.show()


import pandas as pd
df = spark.read.option("header","true").csv("/bolsa.csv")
df
dfpd = df.toPandas()
dfpd


import pandas as pd
dfpd = pd.read_csv("bolsa.csv")
df = spark.createDataFrame(dfpd)


df = spark.read.format("json").load("/flight-data.json")
df.printSchema()


from pyspark.sql.types import StructField, StructType, StringType, LongType
myManualSchema = StructType([
  StructField("DEST_COUNTRY_NAME", StringType(), True),
  StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
  StructField("count", LongType(), False, metadata={"hello":"world"})
])
df = spark.read.format("json").schema(myManualSchema).load("/flight-data.json")

from pyspark.sql.types import StructField, StructType, StringType, LongType
from pyspark.sql.functions import col, column
myManualSchema = StructType([
  StructField("DEST_COUNTRY_NAME", StringType(), True),
  StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
  StructField("count", LongType(), False, metadata={"hello":"world"})
])
df = spark.read.format("json").schema(myManualSchema).load("/flight-data.json")
df.col("ORIGIN_COUNTRY_NAME")
df.column("ORIGIN_COUNTRY_NAME")

