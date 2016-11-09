# Homework: Hash-based aggregation in Spark

In this assignment you'll implement hash-based aggregation in [Apache Spark](http://spark.apache.org). This project will illustrate key concepts in parallel data aggregation.

The assignment due date is published at the [class website](https://sites.google.com/site/cs143databasesystems/).

You can complete this **in pairs**, if you choose to. 
Lastly, there is a lot of code in this directory. Please look [here](https://github.com/ariyam/cs143_spark_hw/tree/master/sql) here to find the directory where the code is located.

## Assignment Goals

1. *Implement hash-base aggregation*
2. *Implement spillable data strucuture*
3. *Combine the above two tecniques to implement hybrid hashing aggregation*
4. *Implement AVG using our hybrid hasing implementation*


# Setup

As the UDF caching homework.

# Background and Framework

## Aggregation

* TODO Blob on parallel aggregation.
 

## Project Framework

All the code you will be touching will be in four files -- `CS143Utils.scala`, `SparkStrategies.scala`, `aggregates.scala` `SpillableAggregate.scala`. You might however need to consult other files within Spark (especially `Aggregate.scala`) or the general Scala APIs in order to complete the assignment thoroughly. Please make sure you look through *all* the provided code in the four files mentioned above before beginning to write your own code. There are a lot of useful functions in `CS143Utils.scala` that will save you a lot of time and cursing -- take advantage of them!

In general, we have defined most (if not all) of the methods that you will need. As before, in this project, you need to fill in the skeleton. The amount of code you will write is not very high -- the total staff solution is less than a 100 lines of code (not including tests). However, stringing together the right components in an efficient way will require some thought and careful planning.


# Your Task

## In memory hash-based aggregation

We have provided you skeleton code for `SpillableAggregate.scala`. This file has 4 important things:
* `aggregator` extracts the physical aggregation from the logical plan;
* `aggregatorSchema` contains the output schema for the aggregate at hand
* the method `newAggregatorInstance` creates the actual instance of the physical aggregator that will be used during execution
* `generateIterator` is the main method driving the computation of the aggregate.

### Task #1: Implementing the in-memory part of `SpillableAggregate`

First, you will need to implement the `aggregator`, `aggregatorSchema`, and `newAggregatorInstance` methods in `SpillableAggregate.scala` for this part. This is a simple exercise, just check `Aggregate.scala`. Try however to understand the logic because you will need those methods to implement `generateIterator`.
In order to complete the implementatition of `generateIterator` at this point, you will need to fill the `hasNext`, `next`, `aggregate` and the `object AggregateIteratorGenerator` in `CS143Utils`. The logic of `generateIterator` is the following: 1) drain the input iterator into the aggregation table;
2) generate an aggregate iterator using the helper function `AggregateIteratorGenerator` properly formatting the aggregate result; and
3) use the Iterator inside `generateIterator` as external interface to access and drive the aggregate iterator. 

No need to spill to disk at this point.

## Hybrid hash aggregation

### Task #2: Make the aggregate table spill to disk

This part of the assignment will reuse part of your code from the UDF assignment.  Your task is first to implement the `maybeSpill` method in `CS143Utils`. The next step is to revise your implementation of `generateIterator` in `SpillableAggregate.scala` by making the current aggregation table check if it can safely add a new record without triggering the spilling to disk. If the record cannot be added to the aggregation table, it will be spilled to disk. To implement the above logic, you will have to fill up the methods `initSpills` and `spillRecord`, and properly modify your implementation of `aggregate`. In `initSpills` remember to set blockSize to 0, otherwise spilled records will stay in memory and not actually spill to disk!

### Task #3: Recursive Aggregation

At this point the only missing part for having an hybrid-hash aggregate is to recursively aggregate records previously spilled to disk. If you have implemented the previous task correctly, to finish this task you will only have to take care of the situation in which 1) the input iterator of `generateIterator` is drained; 2) aggregate table contains aggregate values for a subset of the groups; and 3) the remaining groups sit on disk in files properly partitioned. The idea now is to clear the current aggregation table and fetch the spilled records partition by partition and aggregate them in memory. Implement `fetchSpill` and revise your implementation of `hasNext` and `next` in `generateIterator`. You can assume that the aggregate table for each partition fits in memory (what would you do if instead we remove this assumption?).

## Plugging the hybrid hash aggregate into Spark query planner 

At this point we are almost done with the assignment, we only need to plug our aggregate into the Spark planner and test it with a custom aggregate function.

### Task #4: Adding Spillable Aggregate to Spark query planner

Open `SparkStrategies.scala` and locate `object HashAggregation`: this is the strategy that Spark planner uses to translate logical aggregate operator to their physical implementation. Implement possiblities 2) and 3) using possibility 1) as a reference. 

### Thought Exercise: 
Note how Spark recognize combiner and reducer side aggregation: i.e., using the partial flag. Why Spark have to use two different aggregates for implementing combiner and reducer? The answer to this question is the `class Average` in `aggregates.scala`.

### Task #5: Implementing Average aggregate
As a final exercize we have to implement the `AverageFunction` in `aggregates.scala`. This class implementes the execution logic that will be triggered by our spillable aggregate when an AVG aggregate is used in a Spark SQL query. Fill up all the methods labeled with IMPLEMENT ME. 

At this point, you should be passing ***all*** given tests.


## Testing

We have provided you some sample tests in ``. These tests can guide you as you complete this project. However, keep in mind that they are *not* comprehensive, and you are well advised to write your own tests to catch bugs. Hopefully, you can use these tests as models to generate your own tests. 

In order to run our tests, we have provided a simple Makefile. In order to run the tests for task 1, run `make t1`. Correspondingly for task, run `make t2`, and the same for all other tests. `make all` will run all the tests. 

### Assignment submission

Submission link will be created on CCLE, where you can submit your code by the due date.

### Acknowledgements
Big thanks to Matteo Interlandi.


**Good luck!**
