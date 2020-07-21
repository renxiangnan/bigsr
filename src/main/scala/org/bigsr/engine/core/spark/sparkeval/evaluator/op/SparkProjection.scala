package org.bigsr.engine.core.spark.sparkeval.evaluator.op

import org.apache.spark.rdd.RDD
import org.bigsr.fwk.common.Fact
import org.bigsr.fwk.program.compiler.algebra.{Op, Projection}
import org.bigsr.fwk.program.formula.atom.Term

/**
  * @author xiangnan ren
  */
private[op]
class SparkProjection(override val relationName: String,
                      override val schema: Seq[Term],
                      subOp: Op)
  extends Projection(relationName, schema, subOp) {
  def eval(inputRDD: RDD[Fact]): RDD[Fact] = {
    inputRDD.mapPartitions(iter =>
      for (fact <- iter) yield indices.map(i => fact(i)))
  }
}
