package com.reena.explore.utils
import com.reena.explore.utils.SparkUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class SparkUtilsTest extends AnyFunSuite {
    test("check spark config") {
        assert(SparkUtils.sparkConf().isInstanceOf[SparkConf])
    }

    test("check spark session") {
        val sconf = SparkUtils.sparkConf()
        assert(SparkUtils.sparkSession(sconf).isInstanceOf[SparkSession])
    }
}
