package com.reena.explore.mlpipeline

import com.reena.explore.data.SocialNetworkAdData
import com.reena.explore.io.Reader
import com.reena.explore.ml.pipeline.{Estimator, Evaluator, MLUtils, Transformer}
import com.reena.explore.utils.SparkUtils
import org.scalatest.funsuite.AnyFunSuite
import org.apache.log4j._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{CountVectorizer, HashingTF, IDF, Imputer, Tokenizer}
import org.apache.spark.sql.{SaveMode, functions}

case class SampleData(feature_1: Double, feature_2: String, feature_3: String, feature_4: Int, label: Double)
case class SimpleData(_c1: Double, _c2: Double)

class MLPipelineTest extends AnyFunSuite{

    val spark = SparkUtils.sparkSession(SparkUtils.sparkConf())
    Logger.getLogger("org").setLevel(Level.ERROR)

    import spark.implicits._
    val snaDF = Reader.read(spark, "/Users/reenarawat/Downloads/SparkScala/HeadFirstSpark/data/socialNetworkAd", "csv")
    val snaDS = snaDF.as[SocialNetworkAdData.SocialNetworkAd]
ß
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

        //Save model to PipelineModel
        val modelPath = "/Users/reenarawat/Downloads/SparkScala/HeadFirstSpark/data/output/model"
        model.write.overwrite().save(modelPath)

        val results = model.transform(sampleDataDF)

        results.select("features", "label", "rawPrediction", "probability", "prediction").show()

        val evaluator = Evaluator.binaryClassificationEvaluator(dependentVariable,
            "rawPrediction",
            "areaUnderROC")

        val accuracy = evaluator.evaluate(results)
        println("Accuracy = " + accuracy)
    }

    ignore("load model from path") {

        val modelPath = "/path/to/trained/model"
        val model = PipelineModel.load(modelPath)

        val inputDataPath = "/path/to/input/data"
        val inputData = spark.read.format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(inputDataPath)

        val predictions = model.transform(inputData)
        predictions.show()

        val outputPath = "/path/to/output/predictions"
        predictions.write.format("csv")
          .mode(SaveMode.Overwrite)
          .option("header", "true")
          .save(outputPath)
    }

    test("impute missing values on input data") {
        val inputDF = spark.createDataFrame(Seq(
            SimpleData(1.0, 0),
            SimpleData(2.0, 0),
            SimpleData(0, 3.0),
            SimpleData(4.0, 4.0),
            SimpleData(5.0, 5.0)
        )).toDF("_c1", "_c2")

        val inputCols = Array("_c1", "_c2")

        val imputedDataDF = new Imputer()
          .setStrategy("median")
          .setMissingValue(0)
          .setInputCols(inputCols)
          .setOutputCols(inputCols.map( col => s"${col}_imputed"))
          .fit(inputDF)
          .transform(inputDF)

        imputedDataDF.show()
    }

    test("impute missing values on label data") {

        val inputDF = spark.createDataFrame(Seq(
            SimpleData(1.0, 0),
            SimpleData(2.0, 0),
            SimpleData(0, 3.0),
            SimpleData(4.0, 4.0),
            SimpleData(5.0, 5.0)
        )).toDF("_c1", "_c2")

        val imputer = new Imputer()
          .setStrategy("median")
          .setMissingValue(0)
          .setInputCols(Array("_c1", "_c2"))
          .setOutputCols(Array("_c1_out", "_c2_out"))

        val label = Seq(
            SimpleData(6.0, 0),
            SimpleData(0, 2.0),
        ).toDS()

        val model = imputer.fit(inputDF)
        val data = model.transform(label)
        data.show()
    }

    test("handle outliers data") {
        val inputDF = spark.createDataFrame(Seq(
            SimpleData(1.0, 0),
            SimpleData(2.0, 0),
            SimpleData(0, 94.0), // contains outlier
            SimpleData(4.0, 5.0),
            SimpleData(0, 6.0),
            SimpleData(3.0, 0),
            SimpleData(50.0, 5.0) //// contains outlier
        )).toDF("_c1", "_c2")

        //impute missing values
        val inputCols = Array("_c1", "_c2")
        val imputedDataDF = new Imputer()
          .setStrategy("median")
          .setMissingValue(0)
          .setInputCols(inputCols)
          .setOutputCols(inputCols.map( col => s"${col}_imputed"))
          .fit(inputDF)
          .transform(inputDF)

        imputedDataDF.show()

        //calculate mean and stddev and as stats as new cols.
        val outlierCols = Array("_c1_imputed", "_c2_imputed")
        val stats = imputedDataDF.select( outlierCols.map( col => functions.mean(col).alias(s"${col}_mean")) ++
          outlierCols.map( col => functions.stddev(col).alias(s"${col}_stddev")):_*)
          .collect()
          .head

        //Set threshold as 2 std devs from the mean
        val threshold = 2.0

        //Filter the outliers
        val filteredData = imputedDataDF.filter {
            row => val values = outlierCols.map(col => row.getAs[Double](col))
            values.zipWithIndex.forall{ case(value, index) =>
                math.abs(value - stats.getDouble(index)) <= threshold * stats.getDouble(index + outlierCols.length) }
        }

        filteredData.show()
    }

    test("feature extraction") {
        val inputDataDF = spark.createDataFrame(Seq(
            (0, "Spark MLlib is a powerful library for machine learning"),
            (1, "MLlib includes various feature extraction techniques"),
            (2, "Feature extraction is an important step in machine learning")
        )).toDF("id", "text")

        //Tokenization
        val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")

        val tokenizedDataDF = tokenizer.transform(inputDataDF)
        tokenizedDataDF.show(truncate = false)

        //Feature extraction - CountVectorizer
        val cv = new CountVectorizer().setInputCol("words").setOutputCol("features")
        val cvModel = cv.fit(tokenizedDataDF)
        val cvDF = cvModel.transform(tokenizedDataDF)
        cvDF.show(truncate = false)

        //Feature extraction - HashingDF
        val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(1000)
        val featuredDataDF = hashingTF.transform(tokenizedDataDF)

        featuredDataDF.show(truncate = false)

        //Feature extraction - IDF
        val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
        val idfModel = idf.fit(featuredDataDF)
        val idfDF = idfModel.transform(featuredDataDF)

        idfDF.show(truncate = false)

    }
}
