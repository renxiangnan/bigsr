package org.bigsr.engine.core.flink.flinkeval.evaluator

import org.bigsr.engine.core.flink.common.FlinkStreamMap
import org.bigsr.fwk.program.Stratum
import org.bigsr.fwk.program.graph.stratify.{DAGVertexParam, SCCVertex}

/**
  * @author xiangnan ren
  */
private[flink]
class FlinkStratumEval(@transient
                       val param: DAGVertexParam)
  extends Ordered[FlinkStratumEval] {

  override def toString: String = param.toString

  override def compare(that: FlinkStratumEval): Int = this.id - that.id

  def id: Int = param.id

  def grounding(db: FlinkStreamMap): FlinkStreamMap = nonRecursive(db)

  private def nonRecursive(db: FlinkStreamMap): FlinkStreamMap = {
    require(param.combinedAlgebra.size == 1,
      "The non-recursive stratum should have unique IDB relation")

    val idbName = param.combinedAlgebra.head._1
    val root = param.combinedAlgebra.head._2
    val idb = FlinkAlgebraEvaluator.evaluate(root, db)
    db.update(idbName, idb)
    db
  }

}

object FlinkStratumEval {
  def apply(stratum: Stratum,
            sccVertex: SCCVertex,
            edbPredicates: Set[String]): FlinkStratumEval = {
    val param = DAGVertexParam(stratum.rules, sccVertex, edbPredicates)
    new FlinkStratumEval(param)
  }

}