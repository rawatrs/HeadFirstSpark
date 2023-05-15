package com.reena.explore.mlpipeline

import com.reena.explore.data.SocialNetworkAdData
import com.reena.explore.io.Reader
import com.reena.explore.ml.pipeline.{Estimator, Evaluator, MLUtils, Transformer}
import com.reena.explore.utils.SparkUtils
import org.scalatest.funsuite.AnyFunSuite
import org.apache.log4j._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator

case class SampleData(feature_1: Double, feature_2: String, feature_3: String, feature_4: Int, label: Double)

class MLPipelineTest extends AnyFunSuite{

    val spark = SparkUtils.sparkSession(SparkUtils.sparkConf())
    Logger.getLogger("org").setLevel(Level.ERROR)

    import spark.implicits._
    val snaDF = Reader.read(spark, "/Users/reenarawat/Downloads/SparkScala/HeadFirstSpark/data/socialNetworkAd", "csv")
    val snaDS = snaDF.as[SocialNetworkAdData.SocialNetworkAd]

    test("create ml pipeline") {

        snaDF.printSchema()
        //snaDF.show(4)

        //Split data into train and test
        val (train, test) = MLUtils.splitIntoTestAndTrain(snaDF, 0.8, 0.2)

        //Create pipeline stages - Transformers
        val genderIndexer = Transformer.stringIndexer("Gender", "GenderIndex")
        val genderOneHotEncoder = Transformer.oneHotEncoder("GenderIndex", "GenderOHE")

        val features = Array("GenderOHE", "Age", "EstimatedSalary")
        val dependentVariable = "Purchased"

        val vectorAssembler = Transformer.vectorAssembler(features, "features")
        val scaler = Transformer.standardScaler("features", "scaledFeatures")

        //Create pipeline stage - Estimator
        val logisticRegression = Estimator.logisticRegression("scaledFeatures", dependentVariable)

        //Create Pipeline
        val stages = Array(genderIndexer, genderOneHotEncoder, vectorAssembler, scaler, logisticRegression)
        val pipeline = new Pipeline().setStages(stages)

        //Train the model
        val model = pipeline.fit(train)

        //Get inference / predictions from the trained model
        val results = model.transform(test)
        results.show()

        //Evaluate how the model performed
        val evaluator = new BinaryClassificationEvaluator()
          .setLabelCol(dependentVariable)
          .setRawPredictionCol("rawPrediction")
          .setMetricName("areaUnderROC")

        val accuracy = evaluator.evaluate(results)
        println("Model accuracy = " + accuracy)
    }

    test ("ml pipeline steps") {
        //ML Pipeline is an automated ML workflow.
        //Prepare Data
        //Train model
        //Package model
        //Validate model
        //Deploy model
        //Monitor model
    }

    test("create another ml pipeline") {
        val sampleDataDS = spark.createDataset( Seq(
            SampleData(2.0, "A", "S10", 40, 1.0),
            SampleData(1.0, "X", "E10", 25, 1.0),
            SampleData(4.0, "X", "S20", 10, 0.0),
            SampleData(3.0, "Z", "S10", 20, 0.0),
            SampleData(4.0, "A", "E10", 30, 1.0),
            SampleData(2.0, "Z", "S10", 40, 0.0),
            SampleData(5.0, "X", "B10", 10, 1.0))
        )

        val sampleDataDF = sampleDataDS.toDF()
        sampleDataDF.show()

        val feature_2_Indexer = Transformer.stringIndexer("feature_2", "feature_2Indexer")
        val feature_3_Indexer = Transformer.stringIndexer("feature_3", "feature_3Indexer")
        val feature_2_OHE = Transformer.oneHotEncoder("feature_2Indexer", "feature_2OHE")
        val feature_3_OHE = Transformer.oneHotEncoder("feature_3Indexer", "feature_3OHE")
        val features = Array("feature_1", "feature_2OHE", "feature_3OHE", "feature_4")
        val dependentVariable = "label"

        val vectorAssembler = Transformer.vectorAssembler(features, "features")
        val logisticRegression = Estimator.logisticRegression("features", dependentVariable)

        val stages = Array(feature_2_Indexer,
            feature_3_Indexer,
            feature_2_OHE,
            feature_3_OHE,
            vectorAssembler,
            logisticRegression)

        val pipeline = new Pipeline().setStages(stages)

        val model = pipeline.fit(sampleDataDF)
        val results = model.transform(sampleDataDF)

        results.select("features", "label", "rawPrediction", "probability", "prediction").show()

        val evaluator = Evaluator.binaryClassificationEvaluator(dependentVariable,
            "rawPrediction",
            "areaUnderROC")

        val accuracy = evaluator.evaluate(results)
        println("Accuracy = " + accuracy)
    }
}
