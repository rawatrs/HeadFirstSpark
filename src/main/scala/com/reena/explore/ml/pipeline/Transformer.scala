package com.reena.explore.ml.pipeline
import org.apache.spark.ml.feature.{OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler}

object Transformer {
    //String Indexing is similar to Label Encoding.
    // It assigns a unique integer value to each category.
    // 0 is assigned to the most frequent category,
    // 1 to the next most frequent value, and so on
    def stringIndexer(inputCol : String, outputCol: String) = {
        new StringIndexer().setInputCol(inputCol).setOutputCol(outputCol)
    }

    def oneHotEncoder(inputCol: String, outputCol: String) = {
        new OneHotEncoder().setInputCol(inputCol).setOutputCol(outputCol)
    }

    def vectorAssembler(inputCols: Array[String], outputCol: String) = {
        new VectorAssembler().setInputCols(inputCols).setOutputCol(outputCol)
    }

    def standardScaler(inputCol: String, outputCol: String) = {
        new StandardScaler().setInputCol(inputCol).setOutputCol(outputCol)
    }


}
