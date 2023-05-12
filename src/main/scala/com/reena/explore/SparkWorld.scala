package com.reena.explore

import org.apache.log4j._
import org.apache.spark._

object SparkWorld extends App {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val logFile = "../SparkScalaCourse/data/book.txt"
    val conf = new SparkConf().setAppName("Simple Spark World").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)

    val logData = sc.textFile(logFile).cache()
    val numOfAs = logData.filter(line => line.contains("a")).count()
    val numOfBs = logData.filter(line => line.contains("b")).count()

    println(s"number of Lines containing A = $numOfAs")
    println(s"number of Lines containing B = $numOfBs")

}
