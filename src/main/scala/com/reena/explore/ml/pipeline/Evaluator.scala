package com.reena.explore.ml.pipeline

import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator

object Evaluator {

    def binaryClassificationEvaluator(dependentVariable: String, rawPredictionCol: String, metric: String) = {
      new BinaryClassificationEvaluator()
        .setLabelCol(dependentVariable)
        .setRawPredictionCol(rawPredictionCol)
        .setMetricName(metric)
    }
}
