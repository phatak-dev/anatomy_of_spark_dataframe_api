package com.madhukaraphatak.spark.dataframe

import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StringType, StructField}

/**
 * Dataframe Example
 */
object DataFrameExample {

  def main(args: Array[String]) {

    val sc = new SparkContext(args(0), "Dataframe creation example")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val inMemoryDF = Utils.createDataFrame(sqlContext)
    inMemoryDF.explain(true)
  }
}
