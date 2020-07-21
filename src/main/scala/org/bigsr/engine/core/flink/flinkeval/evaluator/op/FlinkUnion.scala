package org.bigsr.engine.core.flink.flinkeval.evaluator.op

import org.apache.flink.streaming.api.scala.DataStream
import org.bigsr.engine.core.flink.flinkeval.conf.FlinkEnv
import org.bigsr.fwk.common.Fact
import org.bigsr.fwk.program.compiler.algebra.{Op, Union}

/**
  * @author xiangnan ren
  */
class FlinkUnion(leftOp: Op,
                 rightOp: Op) extends Union(leftOp, rightOp) {
  def eval(leftStream: DataStream[Fact],
           rightStream: DataStream[Fact]): DataStream[Fact] = {
    val stream = leftStream.union(rightStream)

    FlinkEnv.setParaLevel(classOf[FlinkUnion].getCanonicalName, stream)
  }
}
