package org.bigsr.engine.core.spark.sparkeval.evaluator

import org.bigsr.engine.core.spark.common.SparkRelationMap
import org.bigsr.fwk.common._

/**
  * @author xiangnan ren
  */

/**
  * Materialized DAG vertex via Spark.
  * This class works like a temporal database to maintain
  * the intermediate result of each strongly connected component
  *
  * @param id     : id of scc
  * @param idbs   : IDB relations of (i)th iteration
  * @param deltas : delta relations of (i+1)th iteration
  * @param edbs   : input EDB relations
  *
  */
private[spark]
class SparkStratumDB(val id: Int,
                     val idbs: SparkRelationMap,
                     val deltas: SparkRelationMap,
                     val edbs: SparkRelationMap) {

  def combinedRelations: SparkRelationMap = {
    idbs ++ deltas ++ edbs
  }

  def unpersistDeltas(): Unit = {
    deltas.foreach { delta =>
      task {
        delta._2.data.unpersist(true)
      }.join()
    }
  }

  def unpersist(name: String, data: SparkRelationMap): Unit =
    data(name).data.unpersist(true)

  def updateEDB(inferredIDBs: SparkRelationMap): SparkStratumDB = {
    inferredIDBs.foreach { case (k: String, _: SparkRelation) =>
      this.edbs.update(k, inferredIDBs(k))
    }
    this
  }

  def emptyDelta: Boolean = {
    if (deltas.isEmpty) true
    else {
      deltas.values.par.foreach { delta =>
        if (!delta.isEmpty)
          delta.isEmpty = delta.data.take(1).length == 0
      }
      deltas.values.forall(_.isEmpty)
    }
  }

  def showAll(numRows: Int): Unit = {
    showIDB(numRows)
    showDelta(numRows)
  }

  def showIDB(numRows: Int): Unit =
    idbs.foreach { case (name: String, relation: SparkRelation) =>
      println(s"IDB: $name")
      relation.show(numRows)
    }

  def showDelta(numRows: Int): Unit =
    deltas.foreach { case (name: String, relation: SparkRelation) =>
      println(s"Delta: $name")
      relation.show(numRows)
    }

  def showEDB(numRows: Int): Unit = {
    edbs.foreach { case (name: String, relation: SparkRelation) =>
      println(s"EDB: $name")
      relation.show(numRows)
    }
  }
}

object SparkStratumDB {
  def apply(id: Int,
            idbs: SparkRelationMap,
            deltas: SparkRelationMap,
            edbs: SparkRelationMap): SparkStratumDB =
    new SparkStratumDB(id, idbs, deltas, edbs)
}
