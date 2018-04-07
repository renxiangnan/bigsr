package org.bigsr.engine.core.flink.flinkeval.evaluator

import org.bigsr.engine.core.flink.common.FlinkStreamMap

/**
  * @author xiangnan ren
  */
private[flink]
class FlinkStratumDB(val id: Int,
                     val idbs: FlinkStreamMap,
                     val edbs: FlinkStreamMap) {
  def combinedRelations: FlinkStreamMap = {
    idbs ++ edbs
  }
}

object FlinkStratumDB {
  def apply(id: Int,
            idbs: FlinkStreamMap,
            edbs: FlinkStreamMap): FlinkStratumDB =
    new FlinkStratumDB(id, idbs, edbs)
}