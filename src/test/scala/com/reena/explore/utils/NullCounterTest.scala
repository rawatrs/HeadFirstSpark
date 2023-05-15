package com.reena.explore.utils

import org.apache.log4j._
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

case class Gadget(id: Option[Int], name: Option[String], cost: Option[Double])
case class GadgetNullCount(id: Option[Long], name: Option[Long], cost: Option[Long])
class NullCounterTest extends AnyFunSuite with BeforeAndAfter{

    val spark = SparkUtils.sparkSession(SparkUtils.sparkConf())
    Logger.getLogger("org").setLevel(Level.ERROR)

    import spark.implicits._

    val gadgetDF = Seq(
      Gadget(Some(1), Some("microwave"), Some(0.00)),
      Gadget(Some(2), Some("iphone"), Some(0.00)),
      Gadget(Some(3), None, Some(0.00)),
      Gadget(None, Some("unidentified"), None),
      Gadget(Some(4), None, None),
      Gadget(Some(6), Some(""), None),
    ).toDS().toDF()

    //gadgetDF.show()

    test("check null counts") {
      val nullCountsDF = NullCounter.nullCount(gadgetDF)
      nullCountsDF.show()

      val expectedDS = Seq(
        GadgetNullCount(Some(1), Some(2), Some(3))
      ).toDS()

      print("Expected")
      expectedDS.show()
      expectedDS.printSchema()

      print("Actual")
      val nullCountsDS = nullCountsDF.as[GadgetNullCount]
      nullCountsDS.printSchema()
//      nullCountsDS.collect() shouldBe (expectedDS.collect())

      nullCountsDS.except(expectedDS).count() shouldBe 0
      //nullCountsDF.except(expectedDF)
    }

}
