SBT=build/sbt
TEST=sql/test:testOnly
T1=org.apache.spark.sql.execution.InMemoryAggregateSuite
T2=org.apache.spark.sql.execution.SpillableAggregationSuite
T3=org.apache.spark.sql.execution.RecursiveAggregationSuite
T4=org.apache.spark.sql.execution.AggregateQuerySuite
T5=org.apache.spark.sql.execution.AverageQuerySuite

compile:
	$(SBT) compile

clean:
	$(SBT) clean

all:
	$(SBT) "$(TEST) $(T1) $(T2) $(T3) $(T4) $(T5)"

t1:
	$(SBT) "$(TEST) $(T1)"

t2:
	$(SBT) "$(TEST) $(T2)"

t3:
	$(SBT) "$(TEST) $(T3)"

t4:
	$(SBT) "$(TEST) $(T4)"

t5:
	$(SBT) "$(TEST) $(T5)"

