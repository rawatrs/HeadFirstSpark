package com.reena.explore.ml.pipeline

import org.apache.spark.sql.DataFrame

object MLUtils {
    def splitIntoTestAndTrain(inputDF: DataFrame, trainDataPercent: Double, testDataPercent: Double) = {
      val split = inputDF.randomSplit(Array(trainDataPercent, testDataPercent), seed = 1234L)
      val train = split(0)
      val test = split(1)
      (train, test)
    }
}
