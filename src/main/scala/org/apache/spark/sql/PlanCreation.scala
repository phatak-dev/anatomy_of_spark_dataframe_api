package org.apache.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GenericRowWithSchema}
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.types.{StringType, StructField, StructType, UTF8String}

/**
 * Created by madhu on 22/7/15.
 */
object PlanCreation {


  def main(args: Array[String]) {

    val sc = new SparkContext(args(0), "Plan creation example")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val inMemoryData = (0 to 500).map(value => {
      val rowValues = Array(value, value + 1, value + 2, value + 3).map(value => value.toString)
      Row.fromSeq(rowValues)
    })

    val inMemoryRDD = sqlContext.sparkContext.makeRDD(inMemoryData)

    val columnNames = List("c1", "c2", "c3", "c4")

    val columnStruct = columnNames.map(colName => StructField(colName, StringType, true))

    val schema = StructType(columnStruct)

    // what's going when you call createDataFrame
    val catalystRDD = inMemoryRDD.map( row => {
      val c1 = UTF8String(row.getString(0))
      val c2 = UTF8String(row.getString(1))
      val c3 = UTF8String(row.getString(2))
      val c4 = UTF8String(row.getString(3))

      val ar = Array(c1,c2,c3,c4).asInstanceOf[Array[Any]]
      new GenericRowWithSchema(ar, schema).asInstanceOf[Row]
    })

    val attributes = schema.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
    //logical plan created
    val logicalRDD = LogicalRDD(attributes,catalystRDD)(sqlContext)
    //create queryExecution which holds all the different plan
    val queryExecutor = sqlContext.executePlan(logicalRDD)
    queryExecutor.assertAnalyzed()
   //print physical plan
    println(queryExecutor.executedPlan)

  }

}
