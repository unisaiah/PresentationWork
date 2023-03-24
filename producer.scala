//spark-shell --packages "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7","com.lihaoyi:ujson_2.11:0.7.1"


import org.apache.log4j.{Level, Logger}

import java.util.Properties
import java.time
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.:+
import scala.sys.process._

Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)


val props = new Properties()
props.put("bootstrap.servers", "ip-172-31-13-101.eu-west-2.compute.internal:9092, ip-172-31-3-80.eu-west-2.compute.internal:9092, ip-172-31-5-217.eu-west-2.compute.internal:9092, ip-172-31-9-237.eu-west-2.compute.internal:9092")
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


val sparkConf = new SparkConf()
sparkConf.set("spark.app.name", "Movie1")
sparkConf.set("spark.master", "local[*]")
val spark = SparkSession.builder().config(sparkConf).config("spark.sql.warehouse.dir", "/../../warehouse/tablespace/external/hive").enableHiveSupport().getOrCreate()



// share file formats presentation
val cmd = Seq("curl", "https://api.mockaroo.com/api/c29efa70?count=1000&key=6f2b6e00")
val data = ujson.read(cmd.!!)

val cmd4 = Seq("curl", "https://api.mockaroo.com/api/662caea0?count=1000&key=6f2b6e00")
val data_pay2 = ujson.read(cmd4.!!)

val cmd5 = Seq("curl", "https://api.mockaroo.com/api/ac6df140?count=1000&key=6f2b6e00")
val data_pay3 = ujson.read(cmd5.!!)

val cmd6 = Seq("curl", "https://api.mockaroo.com/api/bd640860?count=1000&key=6f2b6e00")
val data_pay4 = ujson.read(cmd6.!!)

val cmd7 = Seq("curl", "https://api.mockaroo.com/api/d747f6a0?count=1000&key=6f2b6e00")
val data_pay5 = ujson.read(cmd7.!!)

val cmd8 = Seq("curl", "https://api.mockaroo.com/api/e7a0cca0?count=1000&key=6f2b6e00")
val data_pay6 = ujson.read(cmd8.!!)

val cmd9 = Seq("curl", "https://api.mockaroo.com/api/f7c20880?count=1000&key=6f2b6e00")
val data_pay7 = ujson.read(cmd9.!!)

val cmd10 = Seq("curl", "https://api.mockaroo.com/api/06eea7f0?count=1000&key=6f2b6e00")
val data_pay8 = ujson.read(cmd10.!!)

val cmd11 = Seq("curl", "https://api.mockaroo.com/api/1882b850?count=1000&key=6f2b6e00")
val data_pay9 = ujson.read(cmd11.!!)

val cmd12 = Seq("curl", "https://api.mockaroo.com/api/2b1ff750?count=1000&key=6f2b6e00")
val data_pay10 = ujson.read(cmd12.!!)





import spark.implicits._
case class store_payments(film_id: Int, payment_id: Int, amount: Double)

var sq = Seq(store_payments(data.apply(0)("film_id").toString().toInt, data.apply(0)("payment_id").toString().toInt, data.apply(0)("amount").toString().toDouble))
var sq_pay2 = Seq(store_payments(data_pay2.apply(0)("film_id").toString().toInt, data_pay2.apply(0)("payment_id").toString().toInt, data_pay2.apply(0)("amount").toString().toDouble))
var sq_pay3 = Seq(store_payments(data_pay3.apply(0)("film_id").toString().toInt, data_pay3.apply(0)("payment_id").toString().toInt, data_pay3.apply(0)("amount").toString().toDouble))
var sq_pay4 = Seq(store_payments(data_pay4.apply(0)("film_id").toString().toInt, data_pay4.apply(0)("payment_id").toString().toInt, data_pay4.apply(0)("amount").toString().toDouble))
var sq_pay5 = Seq(store_payments(data_pay5.apply(0)("film_id").toString().toInt, data_pay5.apply(0)("payment_id").toString().toInt, data_pay5.apply(0)("amount").toString().toDouble))
var sq_pay6 = Seq(store_payments(data_pay6.apply(0)("film_id").toString().toInt, data_pay6.apply(0)("payment_id").toString().toInt, data_pay6.apply(0)("amount").toString().toDouble))
var sq_pay7 = Seq(store_payments(data_pay7.apply(0)("film_id").toString().toInt, data_pay7.apply(0)("payment_id").toString().toInt, data_pay7.apply(0)("amount").toString().toDouble))
var sq_pay8 = Seq(store_payments(data_pay8.apply(0)("film_id").toString().toInt, data_pay8.apply(0)("payment_id").toString().toInt, data_pay8.apply(0)("amount").toString().toDouble))
var sq_pay9 = Seq(store_payments(data_pay3.apply(0)("film_id").toString().toInt, data_pay9.apply(0)("payment_id").toString().toInt, data_pay9.apply(0)("amount").toString().toDouble))
var sq_pay10 = Seq(store_payments(data_pay10.apply(0)("film_id").toString().toInt, data_pay10.apply(0)("payment_id").toString().toInt, data_pay10.apply(0)("amount").toString().toDouble))

for (i <- 1 to 999) {
    sq = sq ++ Seq(store_payments(data.apply(i)("film_id").toString().toInt, data.apply(i)("payment_id").toString().toInt, data.apply(i)("amount").toString().toDouble))

    sq_pay2 = sq_pay2 ++ Seq(store_payments(data_pay2.apply(i)("film_id").toString().toInt, data_pay2.apply(i)("payment_id").toString().toInt, data_pay2.apply(i)("amount").toString().toDouble))
    sq_pay3 = sq_pay3 ++ Seq(store_payments(data_pay3.apply(i)("film_id").toString().toInt, data_pay3.apply(i)("payment_id").toString().toInt, data_pay3.apply(i)("amount").toString().toDouble))
    sq_pay4 = sq_pay4 ++ Seq(store_payments(data_pay4.apply(i)("film_id").toString().toInt, data_pay4.apply(i)("payment_id").toString().toInt, data_pay4.apply(i)("amount").toString().toDouble))
    sq_pay5 = sq_pay5 ++ Seq(store_payments(data_pay5.apply(i)("film_id").toString().toInt, data_pay5.apply(i)("payment_id").toString().toInt, data_pay5.apply(i)("amount").toString().toDouble))
    sq_pay6 = sq_pay6 ++ Seq(store_payments(data_pay6.apply(i)("film_id").toString().toInt, data_pay6.apply(i)("payment_id").toString().toInt, data_pay6.apply(i)("amount").toString().toDouble))
    sq_pay7 = sq_pay7 ++ Seq(store_payments(data_pay7.apply(i)("film_id").toString().toInt, data_pay7.apply(i)("payment_id").toString().toInt, data_pay7.apply(i)("amount").toString().toDouble))
    sq_pay8 = sq_pay8 ++ Seq(store_payments(data_pay8.apply(i)("film_id").toString().toInt, data_pay8.apply(i)("payment_id").toString().toInt, data_pay8.apply(i)("amount").toString().toDouble))
    sq_pay9 = sq_pay9 ++ Seq(store_payments(data_pay9.apply(i)("film_id").toString().toInt, data_pay9.apply(i)("payment_id").toString().toInt, data_pay9.apply(i)("amount").toString().toDouble))
    sq_pay10 = sq_pay10 ++ Seq(store_payments(data_pay10.apply(i)("film_id").toString().toInt, data_pay10.apply(i)("payment_id").toString().toInt, data_pay10.apply(i)("amount").toString().toDouble))
}

val sq_pay_final = sq ++ sq_pay2 ++ sq_pay3 ++ sq_pay4 ++ sq_pay5 ++ sq_pay6 ++ sq_pay7 ++ sq_pay8 ++ sq_pay9 ++ sq_pay10

val df_payment_new = sq_pay_final.toDF()
df_payment_new.show()


//kafka-topics --bootstrap-server ip-172-31-13-101.eu-west-2.compute.internal:9092, ip-172-31-3-80.eu-west-2.compute.internal:9092, ip-172-31-5-217.eu-west-2.compute.internal:9092, ip-172-31-9-237.eu-west-2.compute.internal:9092 --create --topic movieproj_payment_new
println("Sending Records in Kafka Topic movieproj_payment_new")
df_payment_new.selectExpr("CONCAT( CAST(film_id AS STRING), ',', CAST(payment_id AS STRING), ',', CAST(amount AS STRING)) AS value").write.format("kafka").option("topic", "movieproj_payment_new").option("kafka.bootstrap.servers", "ip-172-31-13-101.eu-west-2.compute.internal:9092, ip-172-31-3-80.eu-west-2.compute.internal:9092, ip-172-31-5-217.eu-west-2.compute.internal:9092, ip-172-31-9-237.eu-west-2.compute.internal:9092").save()

