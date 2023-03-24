import java.util.{Collections, Properties, Date}
import scala.collection.JavaConverters._
import org.apache.spark.sql.functions.{col, split}
import org.apache.spark.sql.functions.to_date
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import java.io.File



Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)

val sparkConf = new SparkConf()
sparkConf.set("spark.app.name", "Movie1")
sparkConf.set("spark.master", "local[*]")
val spark = SparkSession.builder().config(sparkConf).config("spark.sql.warehouse.dir", "/../../warehouse/tablespace/external/hive").enableHiveSupport().getOrCreate()

val props = new Properties()

props.put("bootstrap.servers", "ip-172-31-13-101.eu-west-2.compute.internal:9092, ip-172-31-3-80.eu-west-2.compute.internal:9092, ip-172-31-5-217.eu-west-2.compute.internal:9092, ip-172-31-9-237.eu-west-2.compute.internal:9092")
props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")



println("Receiving Records in Kafka Topic movieproj_payment_new")
val df_payment = spark.read.format("kafka").option("kafka.bootstrap.servers", "ip-172-31-13-101.eu-west-2.compute.internal:9092, ip-172-31-3-80.eu-west-2.compute.internal:9092, ip-172-31-5-217.eu-west-2.compute.internal:9092, ip-172-31-9-237.eu-west-2.compute.internal:9092").option("subscribe", "movieproj_payment_new").option("startingOffset", "earliest").load()
val df_payment1 = df_payment.selectExpr("CAST(value AS STRING) as value")
val df_payment2 = df_payment1.select(split(col("value"), ",").getItem(0).as("film_id"), split(col("value"), ",").getItem(1).as("payment_id"), split(col("value"), ",").getItem(2).as("amount")).drop("value")
val df_payment_final = df_payment2.selectExpr("CAST(film_id AS INT)", "CAST(payment_id AS INT)", "CAST(amount AS DOUBLE)")
df_payment_final.createOrReplaceTempView("payment")
val res_payment = spark.sql("INSERT INTO TABLE movieproject_scala.superset_film_sum SELECT * FROM payment")



