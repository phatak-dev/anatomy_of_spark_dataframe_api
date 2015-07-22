package com.madhukaraphatak.spark.dataframe

import org.apache.spark.sql.types.{StructType, StringType, StructField}
import org.apache.spark.sql.{SQLContext, Row, DataFrame}

/**
 * Created by madhu on 22/7/15.
 */
object Utils {

  def createDataFrame(sqlContext:SQLContext) : DataFrame = {

    val inMemoryData = (0 to 500).map(value => {
      val rowValues = Array(value, value + 1, value + 2, value + 3).map(value => value.toString)
      Row.fromSeq(rowValues)
    })

    val inMemoryRDD = sqlContext.sparkContext.makeRDD(inMemoryData)

    val columnNames = List("c1", "c2", "c3", "c4")

    val columnStruct = columnNames.map(colName => StructField(colName, StringType, true))

    val schema = StructType(columnStruct)

    val inMemoryDF = sqlContext.createDataFrame(inMemoryRDD, schema)
    inMemoryDF

  }

}
