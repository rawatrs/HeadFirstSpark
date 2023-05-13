package com.reena.explore

import com.reena.explore.utils.SparkUtils
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.apache.log4j._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Row}

case class Sale(id: Int, product: String, cost: Double)
case class SmallSale(id: Int, product: String)
class DataFrameTest extends AnyFunSuite with BeforeAndAfter{

    val spark = SparkUtils.sparkSession(SparkUtils.sparkConf())
    Logger.getLogger("org").setLevel(Level.ERROR)

    import spark.implicits._

    val saleDS = Seq(
      Sale(1, "tile", 36.90),
      Sale(2, "tap", 50.00),
      Sale(3, "exhaust", 45.93)
    ).toDS()

    saleDS.show()


  test("create dataframe") {
    val saleDF: DataFrame = saleDS.toDF()
    saleDF.show()
  }

  test("convert dataframe to a non distributed collection") {
    val saleDF: DataFrame = saleDS.toDF()
    saleDF.show()

    val saleArray = saleDF.collect()
    println(saleArray)
    val saleList = saleDF.collectAsList()
    println(saleList)
  }

  test("accessing elements inside dataframe") {
    val saleDF = saleDS.toDF()

    //can we access first row of dataframe? and what does that mean
    val top2Sale = saleDF.head(2)
    println(top2Sale.toList)

    //accessing individual elements of Row (non distributed)

    ////index operations on Row
    println(top2Sale(1).get(2))

    ////Row has a schema == table schema
    println(top2Sale(1).schema)

    ////get a named field from a Row
    val productIdx = top2Sale(1).fieldIndex("product")
    println(top2Sale(1).get(productIdx))

    //accessing/transforming individual elements of Row (distributed)

  }

  test ("mapping a dataframe: needs a row encoder"){
    val saleDF = saleDS.toDF()
    val encoder = RowEncoder(saleDF.schema)
    saleDF.map(row => {
      println(row)
      row
    })(encoder)
  }


  test("mapping a dataframe: saleDF to smallSaleDF") {
    val saleDF = saleDS.toDF()

    val filteredDS = saleDF.filter(row => {
      (row.getInt(0) != 3)
    }
    ).map( row => {
      SmallSale(row.getInt(0), row.getString(1))
    })

    filteredDS.show()

    saleDF.select("id", "product").filter("id != 3").show()

  }

  test("mapping a dataset: saleDS to smallSaleDS") {

    val filteredDS = saleDS.filter(sale => {
      (sale.id != 3)
    }
    ).map(sale => {
      SmallSale(sale.id, sale.product)
    })

    filteredDS.show()

    saleDS.select("id", "product").filter("id != 3").show()

    saleDS.createOrReplaceTempView("sales")

    spark.sql(
      """
        |select id, product
        |from sales
        |where id != 3
        |""".stripMargin).as[SmallSale].show()

  }


}
