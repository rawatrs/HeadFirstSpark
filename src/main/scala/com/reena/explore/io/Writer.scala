package com.reena.explore.io

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Writer {
    def write(dataFrame: DataFrame, mode: SaveMode, path: String, format: String) = {
      dataFrame.write.option("header", "true").mode(mode).format(format).save(path)

    }
}
