package com.madhukaraphatak.spark.dataframe

import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StringType, StructField}

/**
 * Dataframe Example
 */
object DataFrameExample {

  def main(args: Array[String]) {

    val sc = new SparkContext(args(0), "Csv loading example")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val inMemoryData = (0 to 500).map(value => {
      val rowValues = Array(value, value + 1, value + 2, value + 3).map(value => value.toString)
      Row.fromSeq(rowValues)
    })

    val inMemoryRDD = sc.makeRDD(inMemoryData)

    val columnNames = List("c1", "c2", "c3", "c4")

    val columnStruct = columnNames.map(colName => StructField(colName, StringType, true))

    val schema = StructType(columnStruct)

    val inMemoryDF = sqlContext.createDataFrame(inMemoryRDD, schema)

    inMemoryDF.explain(true)


  }



}
