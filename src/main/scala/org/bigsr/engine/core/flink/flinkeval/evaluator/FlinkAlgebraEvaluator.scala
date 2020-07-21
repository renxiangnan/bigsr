package org.bigsr.engine.core.flink.flinkeval.evaluator

import org.apache.flink.streaming.api.scala.DataStream
import org.bigsr.engine.core.flink.common.FlinkStreamMap
import org.bigsr.fwk.common.Fact
import org.bigsr.fwk.program.compiler.algebra.Op

/**
  * @author xiangnan ren
  */
private[evaluator]
object FlinkAlgebraEvaluator {
  def evaluate(op: Op,
               inputRelationMap: FlinkStreamMap): DataStream[Fact] = {
    FlinkOpEvaluator(op, inputRelationMap).eval
  }
}
