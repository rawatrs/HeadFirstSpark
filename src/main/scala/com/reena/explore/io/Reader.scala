package com.reena.explore.io

import com.reena.explore.utils.SparkUtils
import org.apache.spark.sql.SparkSession

object Reader {

    def read(sparkSession: SparkSession, path: String, format: String) = {
      sparkSession.read
        .format(format)
        .option("inferSchema", "true")
        .option("header", "true")
        .load(path)
    }
}
