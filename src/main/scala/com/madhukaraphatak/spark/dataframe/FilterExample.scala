package com.madhukaraphatak.spark.dataframe

import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * Dataframe Example
 */
object FilterExample {

  def main(args: Array[String]) {

    val sc = new SparkContext(args(0), "Optimization example")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val inMemoryDF = Utils.createDataFrame(sqlContext)
    val filteredDF = inMemoryDF.filter("c1 != 0").filter("c2 != 0")
    filteredDF.explain(true)
  }



}
