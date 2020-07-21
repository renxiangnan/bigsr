package org.bigsr.engine.core.spark.sparkeval.evaluator.op

import org.apache.spark.rdd.RDD
import org.bigsr.engine.core.spark.sparkeval.conf.SparkSingleton
import org.bigsr.fwk.common.Fact
import org.bigsr.fwk.program.compiler.algebra.{Join, Op}

/**
  * @author xiangnan ren
  */
private[op]
class SparkJoin(leftOp: Op,
                rightOp: Op) extends Join(leftOp, rightOp) {
  def eval(leftRDD: RDD[Fact],
           rightRDD: RDD[Fact]): RDD[Fact] = {
    val leftRDD_ = leftRDD.mapPartitions { iter =>
      for (i <- iter) yield (i(keyIndices._1), i)
    }
    val rightRDD_ = rightRDD.mapPartitions { iter =>
      for (i <- iter) yield
        (i(keyIndices._2), i.patch(keyIndices._2, Nil, 1))
    }

    val outputRDD = leftRDD_.join(rightRDD_).mapPartitions(iter =>
      for (i <- iter) yield i._2._1 ++ i._2._2)

    if (SparkSingleton.numPartition != SparkSingleton.shuffledPartition)
      outputRDD.coalesce(SparkSingleton.shuffledPartition)
    else outputRDD
  }
}
