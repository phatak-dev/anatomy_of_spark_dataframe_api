package com.madhukaraphatak.spark.dataframe

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.analysis.EliminateSubQueries
import org.apache.spark.sql.catalyst.optimizer._


/**
 * Created by madhu on 22/7/15.
 */
object StepsInQueryPlanning {

  def main(args: Array[String]) {

    val sc = new SparkContext(args(0), "Csv loading example")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)



    val jsonDF = sqlContext.read.json(args(1))


    jsonDF.registerTempTable("sales")


    val sqlDF = sqlContext.sql("select a.customerId from ( " +
      "select customerId , amountPaid as amount  from sales  where 1 = '1') a " +
      " where amount=500.0")

    val logicalPlan = sqlDF.queryExecution.logical

    println(logicalPlan.treeString)

    val analyzer = Utils.getAnalayzer(sqlContext)

    // resolve identifiers

    val resolvedPlan = analyzer.ResolveRelations(logicalPlan)

    println(resolvedPlan.treeString)

    // resolve references

    val resolvedReferencesPlan = analyzer.ResolveReferences(resolvedPlan)

    println(resolvedReferencesPlan.treeString)

    // promote string

    val promoteStringPlan = analyzer.PromoteStrings(resolvedReferencesPlan)

    println(promoteStringPlan.treeString)
    //let's optimize

    val eliminatedPlan = EliminateSubQueries(promoteStringPlan)
    println(eliminatedPlan.treeString)

    //constant folding

    val constantFoldPlan = ConstantFolding(eliminatedPlan)
    println(constantFoldPlan.treeString)

    val simplifiedFiltering = SimplifyFilters(constantFoldPlan)
    println(simplifiedFiltering.treeString)

    val filterPushThrough = PushPredicateThroughProject(simplifiedFiltering)
    println(filterPushThrough.treeString)

    val projectCollapse = ProjectCollapsing(filterPushThrough)
    println(projectCollapse.treeString)


   val phyzicalPlan = new DataFrame(sqlContext,projectCollapse).queryExecution.executedPlan

   println(phyzicalPlan.executeCollect().toList)



  }

}
