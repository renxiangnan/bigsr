package org.bigsr.engine.core.flink.flinkeval.evaluator.op

import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.bigsr.engine.core.flink.flinkeval.conf.{FlinkEnv, FlinkTimeWindowParam}
import org.bigsr.fwk.common.Fact
import org.bigsr.fwk.program.compiler.algebra.{Op, Projection}
import org.bigsr.fwk.program.formula.TimeWindowParam
import org.bigsr.fwk.program.formula.atom.Term

/**
  * @author xiangnan ren
  */
class FlinkProjection(override val relationName: String,
                      override val schema: Seq[Term],
                      subOp: Op,
                      windowParam: Option[TimeWindowParam])
  extends Projection(relationName, schema, subOp) {
  def eval(inputStream: DataStream[Fact]): DataStream[Fact] = {
    val stream = windowParam match {
      case Some(param: FlinkTimeWindowParam) =>
        val s = inputStream.map(fact => for (i <- indices) yield fact(i))
        FlinkOpHelper.distinct(s, param)
      case None =>
        inputStream.map(fact => for (i <- indices) yield fact(i))
      case _ =>
        throw new UnsupportedOperationException("Unknown windowing operator.")
    }

    FlinkEnv.setParaLevel(classOf[FlinkProjection].getCanonicalName, stream)
  }
}
