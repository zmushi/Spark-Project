package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, AllTuples, UnspecifiedDistribution}
import org.apache.spark.util.collection.SizeTrackingAppendOnlyMap

case class SpillableAggregate(
   partial: Boolean,
   groupingExpressions: Seq[Expression],
   aggregateExpressions: Seq[NamedExpression],
   child: SparkPlan) extends UnaryNode {

  override def requiredChildDistribution =
    if (partial) {
      UnspecifiedDistribution :: Nil
    } else {
      if (groupingExpressions == Nil) {
        AllTuples :: Nil
      } else {
        ClusteredDistribution(groupingExpressions) :: Nil
      }
    }

  override def output = aggregateExpressions.map(_.toAttribute)

  /**
   * An aggregate that needs to be computed for each row in a group.
   *
   * @param unbound Unbound version of this aggregate, used for result substitution.
   * @param aggregate A bound copy of this aggregate used to create a new aggregation buffer.
   * @param resultAttribute An attribute used to refer to the result of this aggregate in the final
   *                        output.
   */
  case class ComputedAggregate(
    unbound: AggregateExpression,
    aggregate: AggregateExpression,
    resultAttribute: AttributeReference)

  /** Physical aggregator generated from a logical expression.  */
  private[this] val aggregators: Array[ComputedAggregate] = aggregateExpressions.flatMap { agg =>
    agg.collect {
      case a: AggregateExpression =>
        ComputedAggregate(
          a,
          BindReferences.bindReference(a, child.output),
          AttributeReference(s"aggResult:$a", a.dataType, a.nullable)())
    }
  }.toArray

  /** Physical aggregator generated from a logical expression.  */
  private[this] val aggregator: ComputedAggregate = aggregators(0) //aggregators(aggregators.length-1)

  /** Schema of the aggregate.  */
  private[this] val aggregatorSchema: AttributeReference = aggregator.resultAttribute

  /** Creates a new aggregator instance.  */
  private[this] def newAggregatorInstance(): AggregateFunction = aggregator.aggregate.newInstance()

  /** Named attributes used to substitute grouping attributes in the final result. */
  private[this] val namedGroups = groupingExpressions.map {
    case ne: NamedExpression => ne -> ne.toAttribute
    case e => e -> Alias(e, s"groupingExpr:$e")().toAttribute
  }

  /**
   * A map of substitutions that are used to insert the aggregate expressions and grouping
   * expression into the final result expression.
   */
  protected val resultMap =
    ( Seq(aggregator.unbound -> aggregator.resultAttribute) ++ namedGroups).toMap

  /**
   * Substituted version of aggregateExpressions expressions which are used to compute final
   * output rows given a group and the result of all aggregate computations.
   */
  private[this] val resultExpression = aggregateExpressions.map(agg => agg.transform {
      case e: Expression if resultMap.contains(e) => resultMap(e)
    }
  )

  override def execute() = attachTree(this, "execute") {
    child.execute().mapPartitions(iter => generateIterator(iter))
  }

  /**
   * This method takes an iterator as an input. The iterator is drained either by aggregating
   * values or by spilling to disk. Spilled partitions are successively aggregated one by one
   * until no data is left.
   *
   * @param input the input iterator
   * @param memorySize the memory size limit for this aggregate
   * @return the result of applying the projection
   */
  def generateIterator(input: Iterator[Row], memorySize: Long = 64 * 1024 * 1024, numPartitions: Int = 64): Iterator[Row] = {
    val groupingProjection = CS143Utils.getNewProjection(groupingExpressions, child.output)
    var currentAggregationTable = new SizeTrackingAppendOnlyMap[Row, AggregateFunction]
    var data = input
    var spillIndex = 0;

    def initSpills(): Array[DiskPartition]  = {
      var partitions : Array[DiskPartition] = new Array[DiskPartition](numPartitions)

      partitions
    }

    val spills = initSpills()

    new Iterator[Row] {
      var aggregateResult: Iterator[Row] = aggregate()

      def hasNext() = {
        if (!aggregateResult.hasNext) {
           if (fetchSpill) {
            currentAggregationTable = new SizeTrackingAppendOnlyMap[Row, AggregateFunction]
            aggregateResult = aggregate()
            true
           } else {
            false
           }
        } else {
          true
        }
      }

      def next() = {
        if (!hasNext) {
            throw new java.util.NoSuchElementException
        }

        aggregateResult.next()
      }

      /**
       * This method load the aggregation hash table by draining the data iterator
       *
       * @return
       */
      private def aggregate(): Iterator[Row] = {
        var currFunc:AggregateFunction = null
        
        if (groupingExpressions.isEmpty) {
          currFunc = newAggregatorInstance()

          while(data.hasNext) {
            var currRow = data.next()
            if (CS143Utils.maybeSpill(currentAggregationTable, memorySize)) {
              spillRecord(currRow)
            } else {
              currFunc.update(currRow)
            }
          }

          val resultProjection = new InterpretedProjection(resultExpression, Seq(aggregatorSchema))
          Iterator(resultProjection(Row(currFunc.eval(EmptyRow))))

        } else {
          while(data.hasNext) {
            var currRow = data.next()
            var projectRow = groupingProjection(currRow)
            currFunc = currentAggregationTable(projectRow)

            if (currFunc == null) {
              currFunc = newAggregatorInstance()

              if (CS143Utils.maybeSpill(currentAggregationTable, memorySize)) {
                spillRecord(currRow)
              } else {
                currFunc.update(currRow)
                currentAggregationTable.update(projectRow, currFunc)
              }
            } else {
              currFunc.update(currRow)   
            }
          }
          AggregateIteratorGenerator(
          resultExpression,
          Seq(aggregatorSchema) ++ namedGroups.map(_._2))(currentAggregationTable.iterator)  
        }
      }

      /**
       * Spill input rows to the proper partition using hashing
       *
       * @return
       */
      private def spillRecord(row: Row)  = {
        var indx = row.hashCode() % numPartitions
        if (spills(indx) == null) {
          spills(indx) = new DiskPartition(indx.toString, 0)
        }

        spills(indx).insert(row)
      }

      /**
       * Global object wrapping the spills into a DiskHashedRelation. This variable is
       * set only when we are sure that the input iterator has been completely drained.
       *
       * @return
       */
      var hashedSpills: Option[Iterator[DiskPartition]] = None

      /**
       * This method fetches the next records to aggregate from spilled partitions or returns false if we
       * have no spills left.
       *
       * @return
       */
      private def fetchSpill(): Boolean  = {

        while ((spillIndex < numPartitions) && spills(spillIndex) == null) {
          spillIndex += 1
        }

        if (spillIndex < numPartitions && spills(spillIndex) != null) {
          spills(spillIndex).closeInput()
          data = spills(spillIndex).getData()
          spillIndex += 1
          // Do the aggregation 
          //aggregateResult = aggregate()
          true
        } else {
          false
        }
      }
    }
  }
}
