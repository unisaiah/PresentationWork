import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.sys.process._


object Main {
  def main(a: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkConf = new SparkConf()
    sparkConf.set("spark.app.name", "Movie1")
    sparkConf.set("spark.master", "local[*]")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()


    // Creating dataframes from relevant MySQL tables
    val data1 = spark.read.format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/sakila")
      .option("dbtable", "actor")
      .option("user", "root")
      .option("password", "bob")
      .load()

    val data2 = spark.read.format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/sakila")
      .option("dbtable", "category")
      .option("user", "root")
      .option("password", "bob")
      .load()

//    val data3 = spark.read.format("jdbc")
//      .option("driver", "com.mysql.cj.jdbc.Driver")
//      .option("url", "jdbc:mysql://localhost:3306/sakila")
//      .option("dbtable", "customer")
//      .option("user", "root")
//      .option("password", "bob")
//      .load()

    val data4 = spark.read.format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/sakila")
      .option("dbtable", "film")
      .option("user", "root")
      .option("password", "bob")
      .load()

    val data5 = spark.read.format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/sakila")
      .option("dbtable", "film_actor")
      .option("user", "root")
      .option("password", "bob")
      .load()

    val data6 = spark.read.format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/sakila")
      .option("dbtable", "film_category")
      .option("user", "root")
      .option("password", "bob")
      .load()

//    val data7 = spark.read.format("jdbc")
//      .option("driver", "com.mysql.cj.jdbc.Driver")
//      .option("url", "jdbc:mysql://localhost:3306/sakila")
//      .option("dbtable", "payment")
//      .option("user", "root")
//      .option("password", "bob")
//      .load()

//
//    // Creating dataframes from mockaroo data
//    val cmd1 = Seq("curl", "https://api.mockaroo.com/api/fe8b3bf0?count=1000&key=6f2b6e00")
//    val data_cust = ujson.read(cmd1.!!)
//
//    val cmd2 = Seq("curl", "https://api.mockaroo.com/api/07a8ffb0?count=66&key=6f2b6e00")
//    val data_city = ujson.read(cmd2.!!)
//
//    val cmd3 = Seq("curl", "https://api.mockaroo.com/api/9bd4fe90?count=1000&key=6f2b6e00")
//    val data_pay1 = ujson.read(cmd3.!!)
//
//    val cmd4 = Seq("curl", "https://api.mockaroo.com/api/3d113f80?count=1000&key=6f2b6e00")
//    val data_pay2 = ujson.read(cmd4.!!)
//
//    val cmd5 = Seq("curl", "https://api.mockaroo.com/api/49d1f510?count=1000&key=6f2b6e00")
//    val data_pay3 = ujson.read(cmd5.!!)
//
//    val cmd6 = Seq("curl", "https://api.mockaroo.com/api/52d3ae30?count=1000&key=6f2b6e00")
//    val data_pay4 = ujson.read(cmd6.!!)
//
//    val cmd7 = Seq("curl", "https://api.mockaroo.com/api/5eebb2a0?count=1000&key=6f2b6e00")
//    val data_pay5 = ujson.read(cmd7.!!)
//
//    val cmd8 = Seq("curl", "https://api.mockaroo.com/api/7d833a10?count=1000&key=6f2b6e00")
//    val data_pay6 = ujson.read(cmd8.!!)
//
//    val cmd9 = Seq("curl", "https://api.mockaroo.com/api/87b1ea30?count=1000&key=6f2b6e00")
//    val data_pay7 = ujson.read(cmd9.!!)
//
//    val cmd10 = Seq("curl", "https://api.mockaroo.com/api/a49e0070?count=1000&key=6f2b6e00")
//    val data_pay8 = ujson.read(cmd10.!!)
//
//    val cmd11 = Seq("curl", "https://api.mockaroo.com/api/afc149d0?count=1000&key=6f2b6e00")
//    val data_pay9 = ujson.read(cmd11.!!)
//
//    val cmd12 = Seq("curl", "https://api.mockaroo.com/api/bf57dd90?count=1000&key=6f2b6e00")
//    val data_pay10 = ujson.read(cmd12.!!)
//
//
//    import spark.implicits._
//
//    var sq_cust = Seq(customer(data_cust.apply(0)("customer_id").toString().toInt, data_cust.apply(0)("store_id").toString().toInt, data_cust.apply(0)("city_id").toString().toInt))
//    var sq_city = Seq(city(data_city.apply(0)("city_id").toString().toInt, data_city.apply(0)("city").toString()))
//    var sq_pay1 = Seq(store_payments(data_pay1.apply(0)("payment_id").toString().toInt, data_pay1.apply(0)("store_id").toString().toInt, data_pay1.apply(0)("customer_id").toString().toInt, data_pay1.apply(0)("film_id").toString().toInt, data_pay1.apply(0)("amount").toString().toDouble, data_pay1.apply(0)("date").toString().replace("\"", "")))
//    var sq_pay2 = Seq(store_payments(data_pay2.apply(0)("payment_id").toString().toInt, data_pay2.apply(0)("store_id").toString().toInt, data_pay2.apply(0)("customer_id").toString().toInt, data_pay2.apply(0)("film_id").toString().toInt, data_pay2.apply(0)("amount").toString().toDouble, data_pay2.apply(0)("date").toString().replace("\"", "")))
//    var sq_pay3 = Seq(store_payments(data_pay3.apply(0)("payment_id").toString().toInt, data_pay3.apply(0)("store_id").toString().toInt, data_pay3.apply(0)("customer_id").toString().toInt, data_pay3.apply(0)("film_id").toString().toInt, data_pay3.apply(0)("amount").toString().toDouble, data_pay3.apply(0)("date").toString().replace("\"", "")))
//    var sq_pay4 = Seq(store_payments(data_pay4.apply(0)("payment_id").toString().toInt, data_pay4.apply(0)("store_id").toString().toInt, data_pay4.apply(0)("customer_id").toString().toInt, data_pay4.apply(0)("film_id").toString().toInt, data_pay4.apply(0)("amount").toString().toDouble, data_pay4.apply(0)("date").toString().replace("\"", "")))
//    var sq_pay5 = Seq(store_payments(data_pay5.apply(0)("payment_id").toString().toInt, data_pay5.apply(0)("store_id").toString().toInt, data_pay5.apply(0)("customer_id").toString().toInt, data_pay5.apply(0)("film_id").toString().toInt, data_pay5.apply(0)("amount").toString().toDouble, data_pay5.apply(0)("date").toString().replace("\"", "")))
//    var sq_pay6 = Seq(store_payments(data_pay6.apply(0)("payment_id").toString().toInt, data_pay6.apply(0)("store_id").toString().toInt, data_pay6.apply(0)("customer_id").toString().toInt, data_pay6.apply(0)("film_id").toString().toInt, data_pay6.apply(0)("amount").toString().toDouble, data_pay6.apply(0)("date").toString().replace("\"", "")))
//    var sq_pay7 = Seq(store_payments(data_pay7.apply(0)("payment_id").toString().toInt, data_pay7.apply(0)("store_id").toString().toInt, data_pay7.apply(0)("customer_id").toString().toInt, data_pay7.apply(0)("film_id").toString().toInt, data_pay7.apply(0)("amount").toString().toDouble, data_pay7.apply(0)("date").toString().replace("\"", "")))
//    var sq_pay8 = Seq(store_payments(data_pay8.apply(0)("payment_id").toString().toInt, data_pay8.apply(0)("store_id").toString().toInt, data_pay8.apply(0)("customer_id").toString().toInt, data_pay8.apply(0)("film_id").toString().toInt, data_pay8.apply(0)("amount").toString().toDouble, data_pay8.apply(0)("date").toString().replace("\"", "")))
//    var sq_pay9 = Seq(store_payments(data_pay3.apply(0)("payment_id").toString().toInt, data_pay9.apply(0)("store_id").toString().toInt, data_pay9.apply(0)("customer_id").toString().toInt, data_pay9.apply(0)("film_id").toString().toInt, data_pay9.apply(0)("amount").toString().toDouble, data_pay9.apply(0)("date").toString().replace("\"", "")))
//    var sq_pay10 = Seq(store_payments(data_pay10.apply(0)("payment_id").toString().toInt, data_pay10.apply(0)("store_id").toString().toInt, data_pay10.apply(0)("customer_id").toString().toInt, data_pay10.apply(0)("film_id").toString().toInt, data_pay10.apply(0)("amount").toString().toDouble, data_pay10.apply(0)("date").toString().replace("\"", "")))
//
//    for (i <- 1 to 999) {
//      sq_cust = sq_cust ++ Seq(customer(data_cust.apply(i)("customer_id").toString().toInt, data_cust.apply(i)("store_id").toString().toInt, data_cust.apply(i)("city_id").toString().toInt))
//      sq_pay1 = sq_pay1 ++ Seq(store_payments(data_pay1.apply(i)("payment_id").toString().toInt, data_pay1.apply(i)("store_id").toString().toInt, data_pay1.apply(i)("customer_id").toString().toInt, data_pay1.apply(i)("film_id").toString().toInt, data_pay1.apply(i)("amount").toString().toDouble, data_pay1.apply(i)("date").toString().replace("\"", "")))
//      sq_pay2 = sq_pay2 ++ Seq(store_payments(data_pay2.apply(i)("payment_id").toString().toInt, data_pay2.apply(i)("store_id").toString().toInt, data_pay2.apply(i)("customer_id").toString().toInt, data_pay2.apply(i)("film_id").toString().toInt, data_pay2.apply(i)("amount").toString().toDouble, data_pay2.apply(i)("date").toString().replace("\"", "")))
//      sq_pay3 = sq_pay3 ++ Seq(store_payments(data_pay3.apply(i)("payment_id").toString().toInt, data_pay3.apply(i)("store_id").toString().toInt, data_pay3.apply(i)("customer_id").toString().toInt, data_pay3.apply(i)("film_id").toString().toInt, data_pay3.apply(i)("amount").toString().toDouble, data_pay3.apply(i)("date").toString().replace("\"", "")))
//      sq_pay4 = sq_pay4 ++ Seq(store_payments(data_pay4.apply(i)("payment_id").toString().toInt, data_pay4.apply(i)("store_id").toString().toInt, data_pay4.apply(i)("customer_id").toString().toInt, data_pay4.apply(i)("film_id").toString().toInt, data_pay4.apply(i)("amount").toString().toDouble, data_pay4.apply(i)("date").toString().replace("\"", "")))
//      sq_pay5 = sq_pay5 ++ Seq(store_payments(data_pay5.apply(i)("payment_id").toString().toInt, data_pay5.apply(i)("store_id").toString().toInt, data_pay5.apply(i)("customer_id").toString().toInt, data_pay5.apply(i)("film_id").toString().toInt, data_pay5.apply(i)("amount").toString().toDouble, data_pay5.apply(i)("date").toString().replace("\"", "")))
//      sq_pay6 = sq_pay6 ++ Seq(store_payments(data_pay6.apply(i)("payment_id").toString().toInt, data_pay6.apply(i)("store_id").toString().toInt, data_pay6.apply(i)("customer_id").toString().toInt, data_pay6.apply(i)("film_id").toString().toInt, data_pay6.apply(i)("amount").toString().toDouble, data_pay6.apply(i)("date").toString().replace("\"", "")))
//      sq_pay7 = sq_pay7 ++ Seq(store_payments(data_pay7.apply(i)("payment_id").toString().toInt, data_pay7.apply(i)("store_id").toString().toInt, data_pay7.apply(i)("customer_id").toString().toInt, data_pay7.apply(i)("film_id").toString().toInt, data_pay7.apply(i)("amount").toString().toDouble, data_pay7.apply(i)("date").toString().replace("\"", "")))
//      sq_pay8 = sq_pay8 ++ Seq(store_payments(data_pay8.apply(i)("payment_id").toString().toInt, data_pay8.apply(i)("store_id").toString().toInt, data_pay8.apply(i)("customer_id").toString().toInt, data_pay8.apply(i)("film_id").toString().toInt, data_pay8.apply(i)("amount").toString().toDouble, data_pay8.apply(i)("date").toString().replace("\"", "")))
//      sq_pay9 = sq_pay9 ++ Seq(store_payments(data_pay9.apply(i)("payment_id").toString().toInt, data_pay9.apply(i)("store_id").toString().toInt, data_pay9.apply(i)("customer_id").toString().toInt, data_pay9.apply(i)("film_id").toString().toInt, data_pay9.apply(i)("amount").toString().toDouble, data_pay9.apply(i)("date").toString().replace("\"", "")))
//      sq_pay10 = sq_pay10 ++ Seq(store_payments(data_pay10.apply(i)("payment_id").toString().toInt, data_pay10.apply(i)("store_id").toString().toInt, data_pay10.apply(i)("customer_id").toString().toInt, data_pay10.apply(i)("film_id").toString().toInt, data_pay10.apply(i)("amount").toString().toDouble, data_pay10.apply(i)("date").toString().replace("\"", "")))
//    }
//
//    val sq_pay_final = sq_pay1 ++ sq_pay2 ++ sq_pay3 ++ sq_pay4 ++ sq_pay5 ++ sq_pay6 ++ sq_pay7 ++ sq_pay8 ++ sq_pay9 ++ sq_pay10
//
//    for (i <- 1 to 65) {
//      sq_city = sq_city ++ Seq(city(data_city.apply(i)("city_id").toString().toInt, data_city.apply(i)("city").toString()))
//    }
//
//    val df_cust = sq_cust.toDF()
//    val df_city = sq_city.toDF()
//    val df_pay = sq_pay_final.toDF()
//    df_cust.show()
//    df_city.show()
//    df_pay.show()


    // Create temporary views to allow for SQL Querying
//    data1.createOrReplaceTempView("actor")
    data2.createOrReplaceTempView("category")
    //data3.createOrReplaceTempView("customer")
//    data4.createOrReplaceTempView("film")
//    data5.createOrReplaceTempView("film_actor")
    data6.createOrReplaceTempView("film_category")
    //data7.createOrReplaceTempView("payment")
//    df_cust.createOrReplaceTempView("new_customer")
//    df_city.createOrReplaceTempView("new_city")
//    df_pay.createOrReplaceTempView("new_payment")


    // Write Queries to generate tables for new schema
//    val res_actor = spark.sql("SELECT f.film_id, a.actor_id, first_name, last_name FROM actor a\nJOIN film_actor fa ON a.actor_id = fa.actor_id\nJOIN film f ON fa.film_id = f.film_id\nORDER BY 2, 1")
//    val res_customer = spark.sql("SELECT * FROM new_customer")
//    val res_film = spark.sql("SELECT f.film_id, title, release_year, rental_rate, length, rating, name FROM film f\nJOIN film_category fc ON f.film_id = fc.film_id\nJOIN category c ON fc.category_id = c.category_id\nORDER BY 1")
//    val res_city = spark.sql("SELECT * FROM new_city")
//    val res_payment = spark.sql("SELECT * FROM new_payment")

    val res_category = spark.sql("SELECT * FROM category")
    val res_categoryf = spark.sql("SELECT * FROM film_category")


    // Saves to SQL DATABASE
//    res_actor.write.format("jdbc")
//      .option("driver", "com.mysql.cj.jdbc.Driver")
//      .option("url", "jdbc:mysql://localhost:3306/moviesproject_scala")
//      .option("dbtable", "actor")
//      .option("user", "root")
//      .option("password", "bob")
//      .save()
//
//    res_customer.write.format("jdbc")
//      .option("driver", "com.mysql.cj.jdbc.Driver")
//      .option("url", "jdbc:mysql://localhost:3306/moviesproject_scala")
//      .option("dbtable", "customer")
//      .option("user", "root")
//      .option("password", "bob")
//      .save()
//
//    res_film.write.format("jdbc")
//      .option("driver", "com.mysql.cj.jdbc.Driver")
//      .option("url", "jdbc:mysql://localhost:3306/moviesproject_scala")
//      .option("dbtable", "film")
//      .option("user", "root")
//      .option("password", "bob")
//      .save()
//
//    res_city.write.format("jdbc")
//      .option("driver", "com.mysql.cj.jdbc.Driver")
//      .option("url", "jdbc:mysql://localhost:3306/moviesproject_scala")
//      .option("dbtable", "city")
//      .option("user", "root")
//      .option("password", "bob")
//      .save()
//
//    res_payment.write.format("jdbc")
//      .option("driver", "com.mysql.cj.jdbc.Driver")
//      .option("url", "jdbc:mysql://localhost:3306/moviesproject_scala")
//      .option("dbtable", "payment")
//      .option("user", "root")
//      .option("password", "bob")
//      .save()


    // Saves as a CSV file for use in Hive

//    res_actor.repartition(1).write.format("csv").option("header", true).option("path", "C://Rupa_Path/moviesproj_actor").save()
//    res_customer.repartition(1).write.format("csv").option("header", true).option("path", "C://Rupa_Path/moviesproj_customer").save()
//    res_film.repartition(1).write.format("csv").option("header", true).option("path", "C://Rupa_Path/moviesproj_film").save()
//    res_city.repartition(1).write.format("csv").option("header", true).option("path", "C://Rupa_Path/moviesproj_city").save()
//    res_payment.repartition(1).write.format("csv").option("header", true).option("path", "C://Rupa_Path/moviesproj_payment").save()

    res_category.repartition(1).write.format("csv").option("header", true).option("path", "C://Rupa_Path/moviesproj_category").save()
    res_categoryf.repartition(1).write.format("csv").option("header", true).option("path", "C://Rupa_Path/moviesproj_categoryf").save()



    // We will rename the files manually and then use in producer code to save to hive
  }
}