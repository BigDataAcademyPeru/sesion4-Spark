val df = spark.read.csv("/bolsa.csv")
df
df.show()


val df = spark.read.option("header","true").csv("/bolsa.csv")
df


val df = spark.read.format("json").load("/flight-data.json")
df.printSchema()


import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}
import org.apache.spark.sql.types.Metadata
val myManualSchema = StructType(Array(
  StructField("DEST_COUNTRY_NAME", StringType, true),
  StructField("ORIGIN_COUNTRY_NAME", StringType, true),
  StructField("count", LongType, false,
    Metadata.fromJson("{\"hello\":\"world\"}"))
))
val df = spark.read.format("json").schema(myManualSchema).load("/flight-data.json")


import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.functions.{col, column}
val myManualSchema = StructType(Array(
  StructField("DEST_COUNTRY_NAME", StringType, true),
  StructField("ORIGIN_COUNTRY_NAME", StringType, true),
  StructField("count", LongType, false,
    Metadata.fromJson("{\"hello\":\"world\"}"))
))
val df = spark.read.format("json").schema(myManualSchema).load("/flight-data.json")
df.col("ORIGIN_COUNTRY_NAME")
df.column("ORIGIN_COUNTRY_NAME")
