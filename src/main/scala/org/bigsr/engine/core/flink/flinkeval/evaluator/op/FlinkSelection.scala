package org.bigsr.engine.core.flink.flinkeval.evaluator.op

import org.apache.flink.streaming.api.scala.DataStream
import org.bigsr.engine.core.flink.flinkeval.conf.FlinkEnv
import org.bigsr.fwk.common.Fact
import org.bigsr.fwk.program.compiler.algebra._
import org.bigsr.fwk.program.formula.atom.Term

/**
  * @author xiangnan ren
  */
class FlinkSelection(override val relationName: String,
                     override val schema: Seq[Term])
  extends Selection(relationName, schema) {

  def eval(inputStream: DataStream[Fact]): DataStream[Fact] = {
    val stream =
      if (filterInd.isEmpty) inputStream
      else inputStream.filter { fact =>
        filterInd.forall(i => fact(i) == schema(i).toString)
      }

    FlinkEnv.setParaLevel(classOf[FlinkSelection].getCanonicalName, stream)
  }
}

