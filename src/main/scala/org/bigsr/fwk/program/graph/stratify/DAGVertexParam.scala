package org.bigsr.fwk.program.graph.stratify

import org.bigsr.fwk.program.compiler.algebra.{AlgebraBuilder, Op}
import org.bigsr.fwk.program.formula.atom.Atom
import org.bigsr.fwk.program.graph.DependencyIdDct
import org.bigsr.fwk.program.rule.Rule

import scala.collection.Map
import scala.collection.parallel.{ForkJoinTaskSupport, ParMap}

/**
  * @author xiangnan ren
  */
class DAGVertexParam(val rules: Set[Rule],
                     val isRecursive: Boolean,
                     sccVertex: SCCVertex,
                     edbPredicates: Set[String]) {
  val initAlgebra = initAlgebraMap
  val combinedAlgebra = algebraMap
  val nestedAlgebra = algebraSet

  override def toString: String = {
    s"[id: $id]  [rules: ${rules.mkString(" | ")}]"
  }

  def upstreamOpId(dct: DependencyIdDct): Option[Set[Int]] = {
    dct.upstreamIdDct.get(this.id)
  }

  def id: Int = sccVertex.id

  def downstreamOpId(dct: DependencyIdDct): Option[Set[Int]] = {
    dct.downstreamIdDct.get(this.id)
  }

  def depPredicates: Set[String] = inferredIDBPredicates ++ localEDBPredicates

  def localEDBPredicates: Set[String] =
    allSubgoals.map(_.getPredicate).intersect(edbPredicates)

  def allSubgoals: Set[Atom] = rules.flatMap { r => r.subgoals }.map(_.getAtom)

  /**
    * The method returns inferred IDB predicates from previous DAPOp,
    * i.e.:
    * InferredIDBPredicates = All_P - EDB_P - IDB_P
    */
  def inferredIDBPredicates: Set[String] = {
    val allP = allAtoms.map(_.getPredicate)
    val neededEDBP = allSubgoals.map(_.getPredicate).
      intersect(edbPredicates)

    allP.diff(neededEDBP).diff(idbPredicates)
  }

  def idbPredicates: Set[String] = allHeadAtoms.map(_.predicate)

  private def allAtoms: Set[Atom] =
    rules.flatMap(rule => rule.allAtoms).map(a => a.getAtom)

  def inferredIDBAtoms: Set[Atom] = {
    allSubgoals.filter(a => inferredIDBPredicates.contains(a.getPredicate))
  }

  private def initAlgebraMap: Map[String, Op] = {
    val idbPredicates = allHeadAtoms.map(_.getPredicate)

    groupByPredicate.
      map { case (k: String, rules: Set[Rule]) =>
        k -> AlgebraBuilder(k, idbPredicates, rules, reducedRules).
          createInitAlgebraTree
      }
  }

  def groupByPredicate: Map[String, Set[Rule]] =
    rules.groupBy(_.head.getPredicate)

  /**
    * Only keep the rules for initialize iteration.
    * I.e.,  EDB + InferredDB(from upstream DAG vertex)
    */
  private def reducedRules: Set[Rule] =
    rules.filter { rule => {
      allHeadAtoms.map(_.getPredicate) intersect
        rule.subgoals.map(_.getPredicate)
    }.isEmpty
    }

  def allHeadAtoms: Set[Atom] = rules.map(r => r.head.getAtom)

  private def algebraMap: ParMap[String, Op] = {
    val idbPredicates = allHeadAtoms.map(_.getPredicate)

    val ops = groupByPredicate.
      map { case (k: String, rules: Set[Rule]) =>
        k -> AlgebraBuilder(k, idbPredicates, rules, reducedRules).
          createAlgebraTree
      }.par

    ops.tasksupport = new ForkJoinTaskSupport(
      new scala.concurrent.forkjoin.ForkJoinPool(ops.size))

    ops
  }

  private def algebraSet: ParMap[String, Set[Op]] = {
    val idbPredicates = allHeadAtoms.map(_.getPredicate)

    val groupedIDBRules = rules.filter { rule => {
      allHeadAtoms.map(_.getPredicate) intersect
        rule.subgoals.map(_.getPredicate)
    }.nonEmpty
    }.groupBy(_.head.getPredicate)

    val ops = groupedIDBRules.
      map { case (k: String, rules: Set[Rule]) =>
        k -> AlgebraBuilder(k, idbPredicates, rules, reducedRules).
          createAlgebraTreeSet
      }.par

    if (ops.nonEmpty) {
      ops.tasksupport = new ForkJoinTaskSupport(
        new scala.concurrent.forkjoin.ForkJoinPool(ops.size))
    }

    ops
  }

}

object DAGVertexParam {
  def apply(rules: Set[Rule],
            sccVertex: SCCVertex,
            edbPredicates: Set[String]): DAGVertexParam = {
    if (isRecursive(rules)) new DAGVertexParam(rules, true, sccVertex, edbPredicates)
    else new DAGVertexParam(rules, false, sccVertex, edbPredicates)
  }

  /**
    * The method checks whether the current DAGVertex is
    * recursive or not. Note that the recursion here only concerns
    * a subgraph of the whole program's dependency graph.
    *
    * @param rules : The related rules for current DAGVertex
    */
  private def isRecursive(rules: Set[Rule]): Boolean = {
    val headPredicates = rules.map(r => r.head.getAtom).map(_.getPredicate)
    val bodyAtoms = rules.flatMap(r => r.subgoals.map(_.getPredicate))

    bodyAtoms.intersect(headPredicates).nonEmpty
  }

}