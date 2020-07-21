package org.bigsr.fwk.program

import org.bigsr.fwk.program.graph.stratify.{DAG, SCCVertex}
import org.bigsr.fwk.program.graph.{CompleteDepGraph, DependencyGraph, Negation}
import org.bigsr.fwk.program.rule.Rule

/**
  * @author xiangnan ren
  */
class Program(rules: Set[Rule],
              graph: DependencyGraph) {
  val dag = stratify

  def getDepGraph: DependencyGraph = graph

  def downstreamIds(inputId: Int): Set[Int] = {
    dag.get.dct.downstreamIdDct.getOrElse(inputId, Set.empty)
  }

  def upstreamIds(inputId: Int): Set[Int] = {
    dag.get.dct.upstreamIdDct.getOrElse(inputId, Set.empty)
  }

  def stratify: Option[DAG] = {
    def existCycleNegation(sccVertices: Set[SCCVertex]): Boolean =
      sccVertices.exists { sccVertex =>
        sccVertex.nestedEdges.exists(_.label == Negation.toString)
      }

    val dag = DAG(graph)
    if (!existCycleNegation(dag.sccVertices))
      Some(dag) else None
  }

  def isStratified: Boolean = if (dag.nonEmpty) true else false

  def isRecursive: Boolean =
    try {
      dag.get.sccVertices.size != graph.vertices.size
    } catch {
      case _: Exception =>
        throw NotStratifiedException("" +
          "The given program is not stratified")
    }

  /**
    * Return all the needed EDB predicates
    */
  def getEDBPredicates: Set[String] = {
    val allPredicates = rules.collect { case r: Rule => r.allPredicates }.flatten
    allPredicates.diff(getIDBPredicates)
  }

  def getIDBPredicates: Set[String] = {
    rules.collect { case r: Rule => r.head.getPredicate }
  }

  def gatherRulesByStratum: Set[Stratum] = {
    val collect = (sccVertex: SCCVertex, r: Rule) => {
      for (v <- sccVertex.nestedVertices
           if v.getStdAtom.equals(r.head.getAtom))
        yield r
    }
    val existRules = (sccVertex: SCCVertex,
                      rules: Set[Rule]) => {
      sccVertex.nestedVertices.
        map(_.getStdAtom).intersect(rules.map(_.head.getAtom)).
        isEmpty
    }
    val createStratum = (rules: Set[Rule],
                         sccVertex: SCCVertex) => {
      val rs = (for (r <- rules)
        yield collect(sccVertex, r)).flatten
      Stratum(sccVertex.id, rs)
    }

    try {
      for (sccVertex <- this.dag.get.sccVertices
           if !existRules(sccVertex, rules)) yield
        createStratum(rules, sccVertex)
    } catch {
      case _: Exception => throw NotStratifiedException("" +
        "The given program is not stratified")
    }
  }

}

object Program {
  def apply(rules: Set[Rule]): Program =
    new Program(rules, CompleteDepGraph(rules))
}
