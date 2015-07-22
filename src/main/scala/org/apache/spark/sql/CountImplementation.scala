package org.apache.spark.sql

import com.madhukaraphatak.spark.dataframe.Utils
import org.apache.spark.SparkContext

/**
 * Created by madhu on 22/7/15.
 */
object CountImplementation {

  def main(args: Array[String]) {

    val sc = new SparkContext(args(0), "Count implementation")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val inMemoryDF = Utils.createDataFrame(sqlContext)

    val logicalPlan = inMemoryDF.queryExecution.logical

    val countCommand = CountCommand(logicalPlan)

    val queryExecutorForCount = sqlContext.executePlan(countCommand)

    val executedPlan = queryExecutorForCount.executedPlan

    println(executedPlan)
    //executing the plan

    val count = executedPlan.executeCollect().map(value => value.getLong(0)).head

    println("count value is "+count)


  }





}
