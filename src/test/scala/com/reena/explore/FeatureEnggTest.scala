package com.reena.explore

import com.reena.explore.data.CricketData
import com.reena.explore.io.Reader
import com.reena.explore.utils.SparkUtils
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.apache.log4j._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

case class bespoke(state: String, city: String, count: BigInt)
case class Book(name: String, cost: Int)
case class SampleData(id: Int, category_1: String, category_2: String)

class FeatureEnggTest extends AnyFunSuite with BeforeAndAfter{

    val spark = SparkUtils.sparkSession(SparkUtils.sparkConf())
    Logger.getLogger("org").setLevel(Level.ERROR)

    import spark.implicits._

    val cricketDF = Reader.read(spark, path = "/Users/reenarawat/Downloads/SparkScala/HeadFirstSpark/data/cricket",
        "csv")
    val cricketDS = cricketDF.as[CricketData.Cricket]

    test("tests nothing") {
      println("I am testing nothing!")
    }

  test("create dataframes") {

      val ds1 = spark.range(4)
      ds1.show()
      val ds2 = Seq(11,12,13).toDS()
      ds2.show()

      val ds3 = List(10,20,30).toDS()
      ds3.show()

      val booksDS = Seq(
        Book("Scala", 400),
        Book("Spark", 500),
        Book("Kafka", 600)
      ).toDS()

      booksDS.show()

      val ds4 = spark.createDataset(List((1, "a"),(2, "b"),(3, "c")))
      ds4.show()

      val ds5 = spark.createDataset(Seq(
        Book("Scala", 400),
        Book("Spark", 500),
        Book("Kafka", 600)))

      ds5.show()
  }

  /*
  }

  test("create ML pipeline") {

    val sampleDS = spark.createDataset(Seq(
      SampleData(1, "L101", "R"),
      SampleData(2, "L201", "C"),
      SampleData(3, "D111", "R"),
      SampleData(4, "F210", "C"),
      SampleData(5, "D110", "R")
    ))

    sampleDS.show()

    sampleDS.describe().show()
  }*/

  test("null count dataset") {
    val detailNullCount = cricketDS.map(x => {
      if (x.Detail == None) 1 else 0
    }).reduce(_ + _)

    println(s"detailNullCount=$detailNullCount")
    detailNullCount shouldBe(565)
  }

  test("null count sql") {
    cricketDF.createOrReplaceTempView("cricket")

    spark.sql(
      """
        |select count(*) as Detail from cricket
        |where detail is null
        |""".stripMargin).show()

    spark.sql(
      """
        |select
        | sum(CASE WHEN (detail is null) then 1 else 0 end) as detail_nulls,
        | sum(CASE WHEN (isboundary is null) then 1 else 0 end) as isboundary_nulls
        | from cricket
        |""".stripMargin).show()
  }

  test("null count using dataframe, programmatically iterating columns and rows ") {
  //    find no of nulls or missing value in each column
          def countNulls(columns: Array[String]): Array[Column] = {
            columns.map ( c => {
              count(when(col(c).isNull, c))
              //count(when(col(c).isNull, c)).alias(c)
            })
          }

          cricketDF.select(countNulls(cricketDS.columns):_*).show()

  }


  test("transformations") {
      //check data

    cricketDS.printSchema()
    //drop unwanted columns
      cricketDS.drop("Batsman", "Bowler", "id")

      //explore the data
      cricketDS.select("Isball", "Isboundary", "Runs").describe().show()
      cricketDS.show(3)

      //find no of nulls or missing value in each column
//      def countNulls(columns: Array[String]): Array[Column] = {
//        columns.map ( c => {
//          count(when(c.isNull, c)).alias(c)
//        })
//      }

    //      cricketDS.select(countNulls(cricketDS.columns):_*).show()

  }

  test("1: Binning operation") {
    val testDS = spark.range(1,100)
    testDS.show()

    //testDS.withColumn("idRange", )
  }

  test("2: Onehot encoding") {

  }

  test("3: Aggregation  -- GROUPBY, SUM, MIN, MAX") {

  }

  test("4: Normalization / Standardization") {

  }

  test("5: Polynomial Features") {
  }

  test("6: Interaction Features , eg. area") {
  }

  test("7: Time-based features") {
  }

  test("8: Text-based features") {
  }

  test("9: Geospatial features") {

  }



}
