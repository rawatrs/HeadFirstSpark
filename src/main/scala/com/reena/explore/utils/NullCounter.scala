package com.reena.explore.utils

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame, functions}
import org.apache.spark.sql.functions._

object NullCounter {

  def alterColumnsToCountNulls(columns: Array[Column]): Array[Column] = {
    columns.map(c => {
      sum(when(c.isNull, 1).otherwise(0)).alias(c.toString())
    })
  }


  def nullCount(df : DataFrame): DataFrame = {
    df.show()
    val cols = df.columns.map(strCols => col(strCols))
    val alteredColumns = alterColumnsToCountNulls(cols)
    alteredColumns.foreach(println(_))
    df.select(alteredColumns:_*)

  }

}
