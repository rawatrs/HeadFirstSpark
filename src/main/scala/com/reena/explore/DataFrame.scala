package com.reena.explore

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFrame {
  case class Person(id: Int, name: String, age:Int, friends: Int)

  def main(Args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkConf = new SparkConf()
      .setAppName("org")
      .setMaster("local[*]")

    val sparkSession = SparkSession
      .builder
      .config(sparkConf)
      .getOrCreate()

    import sparkSession.implicits._
    val people = sparkSession.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("../SparkScalaCourse/data/fakeFriends.csv")
      .as[Person]

    println("Inferred schema : ")
    people.printSchema()

    println("")
    people.show(5)
    println("No of records = " + people.count())

    people.select("name").show()
    people.filter(people("age") < 21).show(5)
    people.groupBy("age").count().orderBy(desc("age")).show()

  }
}
