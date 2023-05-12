package com.reena.explore.io

import com.reena.explore.data.SuperMarketData
import com.reena.explore.utils.SparkUtils
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.apache.log4j._
import org.apache.spark.sql.functions._

case class bespoke(state: String, city: String, count: BigInt)

class ReaderTest extends AnyFunSuite with BeforeAndAfter{

    val spark = SparkUtils.sparkSession(SparkUtils.sparkConf())
    Logger.getLogger("org").setLevel(Level.ERROR)

    import spark.implicits._

    val customerDF = Reader.read(spark, "/Users/reenarawat/Downloads/SparkScala/HeadFirstSpark/data/customer",
      "csv")
    val customerDS = customerDF.as[SuperMarketData.Customer]

    val factDF = Reader.read(spark, "/Users/reenarawat/Downloads/SparkScala/HeadFirstSpark/data/fact",
        "csv")
    val factDS = factDF.as[SuperMarketData.FactTable]

    val productDF = Reader.read(spark, "/Users/reenarawat/Downloads/SparkScala/HeadFirstSpark/data/product",
        "csv")
    val productDS = productDF.as[SuperMarketData.Product]

    val transactionDF = Reader.read(spark, "/Users/reenarawat/Downloads/SparkScala/HeadFirstSpark/data/transaction",
      "csv")
    val transactionDS = transactionDF.as[SuperMarketData.Transaction]

  test("Check customer data stats") {
    println("Total Customer Records = " + customerDS.count())
    customerDS.printSchema()
    customerDS.show(5)
    customerDS.select("state").distinct().show()

    //Shows count of all records grouped by state and city combination
    customerDS.groupBy(col("state"), col("city"))
      .count()
      //.select(col("state"), col("city"), col("count"))
      //.select("*")
      .show()

    customerDS.groupBy(col("customername")).count().show()
  }

  test("Check customer data using SQL API") {

      customerDF.createOrReplaceTempView(("customer"))
      val customerAggByStateAndCityDF = spark.sql(
        """
          |select state, city, count(1) as count
          |from customer
          |group by state, city
          |""".stripMargin)

      customerAggByStateAndCityDF.show()

      customerDF.createOrReplaceTempView("customer")

      val filteredCustomerAggDF = spark.sql(
        """
          |select state, city, count(1) as count
          |from customer
          |where state != "gujrat"
          |group by state, city
          |""".stripMargin)

      filteredCustomerAggDF.show()

      val filteredCustomerAggDS = filteredCustomerAggDF.as[bespoke]
      filteredCustomerAggDS.orderBy("count").show()
  }

  test("check fact data") {

    factDS.printSchema()
    factDS.show(5)

    //Sum of sales and profit column, groupedBy customerid
    factDS.groupBy(col("customerid"))
      .agg(sum("sales")
        .alias("totalSales") , sum("profit").alias("totalProfit"))
      .show()

    factDS.groupBy(col("customerid")).agg(Map(
      "sales" -> "sum",
      "profit" -> "max"
    )).show()

    //Sum of all numeric columns grouped By customerid
    factDS.groupBy(col("customerid")).sum().show()
  }

  test("check product data") {
    productDS.printSchema()
    productDS.distinct().count()
    productDS.show(10)
  }

  test("check transaction") {
    transactionDS.printSchema()
    transactionDS.show(5)
    transactionDS.select("payment").distinct().show()
  }



  test("3: combine customer and fact data") {
    val customerFactDS = customerDS.join(factDS,
      customerDS("customerid") === factDS("customerid"), "inner")
      .drop(factDS("customerid"))

    customerFactDS.show()
  }

  test("3: combine product and fact data") {
    val productFactDS = productDS.join(factDS, productDS("productid") === factDS("productid"), "inner")
      .drop(factDS("productid"))

    productFactDS.show()
  }

  /*def toPaymentType(str: String): Int = {
    str match {
      case "cash" => 1
      case "card" => 2
    }
  }*/

  test("7: enrich transaction data") {

    transactionDS.withColumn("timestamp", current_timestamp()).show(5, false)

    val enrichedTransactionDS = transactionDS.withColumn("timestamp", (unix_timestamp() + rand() * 200000000).cast("long"))
      .select(col("transactionid"), col("payment"), to_timestamp(col("timestamp")).alias("timestamp"))
      .withColumn("date", to_date(col("timestamp")))
      .withColumn("year", year(col("timestamp")))
      .withColumn("month", month(col("timestamp")))
      .withColumn("dayofweek", dayofweek(col("timestamp")))
      .withColumn("weekday", date_format(col("timestamp"), "E"))
      .withColumn("hour", hour(col("timestamp")))
      .withColumn("min", minute(col("timestamp")))
      .withColumn("sec", second(col("timestamp")))

    enrichedTransactionDS.show(false)

    //enrichedTransactionDS.withColumn("paymentType", toPaymentType(col("payment").toString()))

    //enrichedTransactionDS.groupBy("year").sum().show(false)


  }



}
