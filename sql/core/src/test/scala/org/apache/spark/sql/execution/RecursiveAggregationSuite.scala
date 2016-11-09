package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Alias, Row, _}
import org.apache.spark.sql.test.TestSQLContext
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.mutable.ArraySeq

class RecursiveAggregationSuite extends FunSuite with BeforeAndAfterAll {
  var list: ArraySeq[(Int, Int)] = null

   // TESTS FOR TASK #3
   test("recursive aggregate") {
     list = new ArraySeq[(Int, Int)](200)

     for (i <- 0 until 200) {
       list(i) = ((i, (i % 4) + 1))
     }

     val attributes: Seq[Attribute] = ScalaReflection.attributesFor[Student]
     val data = TestSQLContext.sparkContext.parallelize(list.map(r => Row(r._1, r._2)), 1)
     val plan: SparkPlan = PhysicalRDD(attributes, data)

     val exp: Expression = count(attributes(1))

     val aggregate = new SpillableAggregate(false, Seq(attributes(0)), Seq(attributes(0), Alias(exp, exp.toString)()), plan)
     val answer = plan.execute().mapPartitions(iter => aggregate.generateIterator(iter))
      assert(answer.map(r => r.getInt(0)).reduce(Math.max(_, _)) == 199)
   }
 }
