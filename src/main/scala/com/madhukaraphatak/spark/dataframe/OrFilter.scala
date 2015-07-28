package com.madhukaraphatak.spark.dataframe

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule


/**
 * Created by madhu on 22/7/15.
 */
object OrFilter {

  object OrFilter extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan transform {
        case Filter(c1, Filter(c2, grandChild)) => {
          Filter(Or(c1, c2), grandChild)
        }
      }
    }
  }

  def main(args: Array[String]) {

    val sc = new SparkContext(args(0), "Or Filter rule")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val inMemoryDF = Utils.createDataFrame(sqlContext)

    val filterDF = inMemoryDF.filter("c1 != 0").filter("c2 != 0")

    val analysedPlan = filterDF.queryExecution.analyzed

    println(analysedPlan.numberedTreeString)

    val transformedPlan = OrFilter(analysedPlan)

    println(transformedPlan.numberedTreeString)


  }

}
