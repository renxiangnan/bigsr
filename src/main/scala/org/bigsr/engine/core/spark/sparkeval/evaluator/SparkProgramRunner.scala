package org.bigsr.engine.core.spark.sparkeval.evaluator

import java.security.InvalidParameterException

import org.apache.spark.sql.SparkSession
import org.bigsr.engine.core.spark.common.SparkEDBMap
import org.bigsr.engine.core.spark.sparkeval.conf.SparkSingleton
import org.bigsr.fwk.common.Utils
import org.bigsr.fwk.program.Program
import org.bigsr.fwk.program.compiler.Runner

/**
  * @author xiangnan ren
  */
class SparkProgramRunner(program: Program)
  extends Runner(program) {
  override type T = SparkStratumEval
  private val plan = initLogicalPlan

  // Initialize the logical with topological sort
  override def initLogicalPlan: Vector[SparkStratumEval] = {
    val dag = program.dag.get
    val strata =
      for (s <- program.gatherRulesByStratum) yield {
        val sccVertex = dag.sccVertices.
          find(sccVertex => sccVertex.id == s.id).get
        SparkStratumEval(s, sccVertex, program.getEDBPredicates)
      }
    strata.toVector.sortBy(x => x)
  }

  def launch(edbMap: SparkEDBMap,
             spark: SparkSession,
             idbPredicate: String): Unit = {
    val res = evalProgram(edbMap, spark, idbPredicate)
      println("#output triples: " + res._1.data.count())
    cleanAllDB(res._2)
  }

  private def evalProgram(edbMap: SparkEDBMap,
                          spark: SparkSession,
                          idbPredicate: String):
  (SparkRelation, Vector[SparkStratumDB]) = {
    val db = plan.map(v => v.initDB(edbMap, spark))
    val lookup = db.indices.map(i => db(i).id -> i).toMap // Id, index

    db.indices.foreach { i =>
      var iter = 0
      do {
        // Trigger local checkpoint, truncate RDD lineage
        if (iter <= SparkSingleton.checkpointThreshold) {
          plan(i).grounding(db(i), false: Boolean);
          iter += 1
        } else {
          plan(i).grounding(db(i), true: Boolean);
          iter = 0
        }
      } while (!db(i).emptyDelta)

      if (i < plan.size - 1 && plan.size > 1)
        updateEDBs(lookup, db(i), db)

      db(i).unpersistDeltas()
    }

    val resultIDB =
      try {
        db.find(s => s.idbs.keySet.contains(idbPredicate)).
          get.idbs(idbPredicate)
      } catch {
        case _: Exception => throw
          new InvalidParameterException("Invalid output IDB predicate.")
      }
    (resultIDB, db)
  }

  /**
    * Update the EDBs for other downstream strongly connected components.
    * If there are more than one downstream components, the recent computed
    * result should be cached.
    */
  private def updateEDBs(lookup: Map[Int, Int],
                         stratumDB: SparkStratumDB,
                         db: Vector[SparkStratumDB]): Unit = {
    stratumDB.idbs.par.foreach { case (name, relation) =>
      program.downstreamIds(stratumDB.id) match {
        case ids => if (ids.size > 1) relation.data.cache().take(1)
          ids.foreach(id => db(lookup(id)).edbs.update(name, relation))
      }
    }
  }

  private def cleanAllDB(db: Vector[SparkStratumDB]): Unit = {
    db.foreach { s =>
      s.idbs.foreach { case (_, idb) => idb.clean() }
      s.edbs.foreach { case (_, edb) => edb.clean() }
    }
  }
}

object SparkProgramRunner {
  def apply(program: Program): SparkProgramRunner = {
    new SparkProgramRunner(program)
  }
}