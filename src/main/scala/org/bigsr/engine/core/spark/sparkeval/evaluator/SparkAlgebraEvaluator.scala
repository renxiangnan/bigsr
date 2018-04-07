package org.bigsr.engine.core.spark.sparkeval.evaluator

import org.apache.spark.rdd.RDD
import org.bigsr.engine.core.spark.common.SparkRelationMap
import org.bigsr.fwk.common.Fact
import org.bigsr.fwk.program.compiler.algebra.Op


/**
  * @author xiangnan ren
  */
private[spark]
object SparkAlgebraEvaluator {
  def evaluate(ops: Set[Op],
               inputRelationMap: SparkRelationMap): RDD[Fact] = {
    ops.filter { op =>
      op.leaves.exists { l => !inputRelationMap(l).isEmpty }
    }.map(op => SparkOpEvaluator(op, inputRelationMap).eval).
      reduce(_ union _)
  }

  def evaluate(op: Op,
               inputRelationMap: SparkRelationMap): RDD[Fact] = {
    SparkOpEvaluator(op, inputRelationMap).eval
  }

}
