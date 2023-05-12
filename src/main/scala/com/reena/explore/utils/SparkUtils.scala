package com.reena.explore.utils

import org.apache.spark._
import org.apache.spark.sql.SparkSession

object SparkUtils {

    def sparkConf() = new SparkConf().setAppName ("ABC").setMaster ("local[*]")
    def sparkSession(conf: SparkConf) = SparkSession.builder.config(sparkConf).getOrCreate()

}
