package org.bigsr.engine.core.flink.flinkeval.evaluator

import org.apache.flink.streaming.api.scala.DataStream
import org.bigsr.engine.core.flink.common.{FlinkEDBMap, FlinkStreamMap}
import org.bigsr.fwk.common.Fact
import org.bigsr.fwk.program.Program
import org.bigsr.fwk.program.compiler.Runner

import scala.collection.mutable

/**
  * @author xiangnan ren
  */
class FlinkRunner(program: Program)
  extends Runner(program) {
  require(!program.isRecursive,
    "Flink runner has not supported recursive program yet.")
  override type T = FlinkStratumEval
  private val plan = initLogicalPlan

  // Topological sort
  def initLogicalPlan: Vector[FlinkStratumEval] = {
    val dag = program.dag.get
    val ops =
      for (s <- program.gatherRulesByStratum)
        yield {
          val sccVertex = dag.sccVertices.
            find(sccVertex => sccVertex.id == s.id).get
          FlinkStratumEval(s, sccVertex, program.getEDBPredicates)
        }
    ops.toVector.sortBy(stratum => stratum)
  }

  def launch(edbMap: FlinkEDBMap): FlinkStreamMap = {
    evalProgram(edbMap)
  }

  private def evalProgram(edbMap: => FlinkEDBMap): FlinkStreamMap = {
    val db = mutable.Map[String, DataStream[Fact]]() ++ edbMap
    plan.foreach(_.grounding(db))
    db
  }

}

object FlinkRunner {
  def apply(program: Program): FlinkRunner = {
    new FlinkRunner(program)
  }
}