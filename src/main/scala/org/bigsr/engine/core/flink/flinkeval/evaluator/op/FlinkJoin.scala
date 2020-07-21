package org.bigsr.engine.core.flink.flinkeval.evaluator.op

import org.apache.flink.streaming.api.scala.DataStream
import org.bigsr.engine.core.flink.flinkeval.conf.{FlinkEnv, FlinkTimeWindowParam}
import org.bigsr.fwk.common.Fact
import org.bigsr.fwk.program.compiler.algebra.{Join, Op}
import org.bigsr.fwk.program.formula.TimeWindowParam

/**
  * @author xiangnan ren
  */
class FlinkJoin(leftOp: Op,
                rightOp: Op,
                windowParam: Option[TimeWindowParam])
  extends Join(leftOp, rightOp) {
  def eval(leftStream: DataStream[Fact],
           rightStream: DataStream[Fact]): DataStream[Fact] = {
    val stream = windowParam match {
      case Some(param: FlinkTimeWindowParam) =>
        FlinkOpHelper.timedWindowJoin(leftStream, rightStream, keyIndices, param)
      case None => FlinkOpHelper.windowlessJoin(leftStream, rightStream, keyIndices)
      case _ => throw new UnsupportedOperationException("Unknown windowing operator.")
    }

    FlinkEnv.setParaLevel(classOf[FlinkJoin].getCanonicalName, stream)
  }
}
