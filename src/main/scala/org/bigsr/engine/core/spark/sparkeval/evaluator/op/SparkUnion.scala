package org.bigsr.engine.core.spark.sparkeval.evaluator.op

import org.apache.spark.rdd.RDD
import org.bigsr.fwk.common.Fact
import org.bigsr.fwk.program.compiler.algebra.{Op, Union}

/**
  * @author xiangnan ren
  */
private[op]
class SparkUnion(leftOp: Op,
                 rightOp: Op) extends Union(leftOp, rightOp) {
  def eval(leftRDD: RDD[Fact],
           rightRDD: RDD[Fact]): RDD[Fact] = {
    leftRDD.union(rightRDD)
  }
}
