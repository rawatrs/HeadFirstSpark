package com.reena.explore.io

import com.reena.explore.data.SuperMarketData
import com.reena.explore.utils.SparkUtils
import org.scalatest.funsuite.AnyFunSuite
import org.apache.log4j._
import org.apache.spark.sql.SaveMode

class WriterTest extends AnyFunSuite {
  val spark = SparkUtils.sparkSession(SparkUtils.sparkConf())
  Logger.getLogger("org").setLevel(Level.ERROR)

  import spark.implicits._

  val productDF = Reader.read(spark, "/Users/reenarawat/Downloads/SparkScala/HeadFirstSpark/data/product",
    "csv")
  val productDS = productDF.as[SuperMarketData.Product]

  test("write data to file as it is") {
    Writer.write(productDF,
      SaveMode.Overwrite,
      "/Users/reenarawat/Downloads/SparkScala/HeadFirstSpark/data/output/product",
      "csv")
  }

  ignore("write to S3") {
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "awsaccesskey value")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "aws secretkey value")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
    productDF.write.parquet("s3a://sparkbyexamples/csv/datacsv")
  }
}
