package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.types.LongType

/**
 * Created by madhu on 22/7/15.
 */
case class CountCommand(
                         logicalPlan: LogicalPlan) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val queryExecution = sqlContext.executePlan(logicalPlan)
    val count = queryExecution.toRdd.count()
    Seq(Row.fromSeq(Seq(count)))
  }

  override def output: Seq[Attribute] = Seq(AttributeReference("count", LongType, nullable = false)())

}
