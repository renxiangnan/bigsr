package org.bigsr.engine.core.spark.sparkeval.evaluator

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.bigsr.engine.core.spark.common.{SparkEDBMap, SparkRelationMap}
import org.bigsr.engine.core.spark.sparkeval.evaluator.SparkStratumEval._
import org.bigsr.fwk.common.Fact
import org.bigsr.fwk.program.Stratum
import org.bigsr.fwk.program.compiler._
import org.bigsr.fwk.program.compiler.algebra.Op
import org.bigsr.fwk.program.formula.atom.Atom
import org.bigsr.fwk.program.graph.stratify.{DAGVertexParam, SCCVertex}

import scala.collection.mutable

/**
  * @author xiangnan ren
  */
private[spark]
class SparkStratumEval(@transient val param: DAGVertexParam)
  extends Ordered[SparkStratumEval] {

  override def compare(that: SparkStratumEval): Int = {
    this.id - that.id
  }

  override def toString: String = param.toString

  def grounding(db: SparkStratumDB,
                checkpoint: Boolean): SparkStratumDB = {
    if (param.isRecursive) recursive(db, checkpoint)
    else nonRecursive(db)
  }

  private def recursive(db: SparkStratumDB,
                        checkpoint: Boolean): SparkStratumDB = {
    val updatedIDBs = mutable.Map[String, SparkRelation]()
    val updatedDeltas = mutable.Map[String, SparkRelation]()

    param.nestedAlgebra.foreach { case (name, ops) =>
      val oldDelta = db.deltas(deltaKey(name))
      val oldIDB = db.idbs(name).data
      if (!oldDelta.isEmpty) {
        val newDelta = SparkAlgebraEvaluator.
          evaluate(ops, db.combinedRelations).
          subtract(oldIDB).distinct()
        val newIDB = oldIDB.union(db.deltas(deltaKey(name)).data)

        updatedIDBs += (name -> SparkRelation(newIDB))
        updatedDeltas += (deltaKey(name) -> SparkRelation(newDelta))

        cacheRelation(oldIDB, oldDelta.data, newIDB, newDelta, checkpoint)
      }
    }

    updatedIDBs.foreach(kv => db.idbs.update(kv._1, kv._2))
    updatedDeltas.foreach(kv => db.deltas.update(kv._1, kv._2))
    db
  }

  private def nonRecursive(db: SparkStratumDB): SparkStratumDB = {
    require(param.combinedAlgebra.size == 1,
      "The non-recursive stratum should have unique IDB relation")
    val idb = SparkAlgebraEvaluator.evaluate(
      param.combinedAlgebra.head._2,
      db.combinedRelations).distinct()
    db.idbs.update(param.combinedAlgebra.head._1, SparkRelation(idb))
    db
  }

  def initDB(edbMap: SparkEDBMap,
             spark: SparkSession): SparkStratumDB = {
    val data = initEDBs(edbMap, spark)
    val idbs = initLocalIDBs(edbMap, spark)
    val delta = initDelta(edbMap, spark)

    SparkStratumDB(id, idbs, delta, toMutable(data))
  }

  def id: Int = param.id

  private def initEDBs(edbMap: SparkEDBMap,
                       spark: SparkSession): SparkRelationMap = {
    val inferredIDBs = param.inferredIDBAtoms.map { a =>
      a.getPredicate -> SparkRelation(createEmptyRDD(a, spark))
    }.toMap

    toMutable(inferredIDBs) ++ edbMap.
      filterKeys(k => param.localEDBPredicates.contains(k))
  }

  private def initLocalIDBs(edbMap: SparkEDBMap,
                            spark: SparkSession): SparkRelationMap = {
    val map = param.allHeadAtoms.map(atom =>
      atom.getPredicate -> {
        SparkRelation(createEmptyRDD(atom, spark))
      }).toMap

    toMutable(map)
  }

  private def initDelta(edbMap: SparkEDBMap,
                        spark: SparkSession): SparkRelationMap = {
    if (param.isRecursive) {
      val map = param.initAlgebra.
        map { case (key: String, op: Op) =>
          deltaKey(key) -> {
            val initialDelta = SparkOpEvaluator(op, toMutable(edbMap)).eval
            initialDelta.cache().count()
            SparkRelation(initialDelta)
          }
        }
      toMutable(map)
    } else scala.collection.mutable.Map()
  }

  private def deltaKey(key: String): String = "DELTA_" + key

}

private[spark]
object SparkStratumEval {
  def apply(stratum: Stratum,
            sccVertex: SCCVertex,
            edbPredicates: Set[String]): SparkStratumEval = {
    val param = DAGVertexParam(stratum.rules, sccVertex, edbPredicates)
    new SparkStratumEval(param)
  }

  private def cacheRelation(oldIDB: => RDD[Fact], oldDelta: => RDD[Fact],
                            newIDB: => RDD[Fact], newDelta: => RDD[Fact],
                            checkpoint: Boolean): Unit = {
    //    oldIDB.unpersist(); oldDelta.unpersist()
    newIDB.persist(StorageLevel.MEMORY_ONLY)
    newDelta.persist(StorageLevel.MEMORY_ONLY)

    if (checkpoint) {
      newIDB.cache().localCheckpoint()
      newDelta.cache().localCheckpoint()
    }
  }

  private def createEmptyRDD(atom: Atom,
                             spark: SparkSession): RDD[Fact] = {
    spark.sparkContext.emptyRDD
  }

}
