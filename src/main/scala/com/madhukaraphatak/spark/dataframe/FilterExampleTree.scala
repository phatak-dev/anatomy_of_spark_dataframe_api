package com.madhukaraphatak.spark.dataframe

import org.apache.spark.SparkContext

/**
 * Dataframe Example
 */
object FilterExampleTree {

  def main(args: Array[String]) {

    val sc = new SparkContext(args(0), "Optimization example")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val inMemoryDF = Utils.createDataFrame(sqlContext)
    val filteredDF = inMemoryDF.filter("c1 != 0").filter("c2 != 0")

    println(filteredDF.queryExecution.analyzed.numberedTreeString)

    println(filteredDF.queryExecution.optimizedPlan.numberedTreeString)
  }



}
