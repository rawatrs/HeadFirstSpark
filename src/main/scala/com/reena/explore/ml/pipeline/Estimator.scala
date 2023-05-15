package com.reena.explore.ml.pipeline

import org.apache.spark.ml.classification.LogisticRegression

object Estimator {
    def logisticRegression(featuresCol: String, labelCol: String) = {
      new LogisticRegression().setFeaturesCol(featuresCol).setLabelCol(labelCol)
    }
}
