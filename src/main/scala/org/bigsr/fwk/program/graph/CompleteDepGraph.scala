package org.bigsr.fwk.program.graph

import org.bigsr.fwk.program.formula.atom.Atom
import org.bigsr.fwk.program.rule.Rule

/**
  * @author xiangnan ren
  */
class CompleteDepGraph(override val vertices: Set[Vertex],
                       override val edges: Set[Edge])
  extends DependencyGraph {
  override type G = CompleteDepGraph

  override def simplify(): CompleteDepGraph = {
    val simplifiedVertices =
      this.vertices.map(v =>
        Vertex(Atom(v.getPredicate)))
    val simplifiedEdges =
      this.edges.map(e => Edge(
        simplifyVertex(e.src),
        simplifyVertex(e.dst), e.label))

    new CompleteDepGraph(simplifiedVertices, simplifiedEdges)
  }

  override def reversedGraph: CompleteDepGraph = {
    val reversedEdges = this.edges.map(_.reversed)
    new CompleteDepGraph(this.vertices, reversedEdges)
  }
}

object CompleteDepGraph {
  def apply(rules: Set[Rule]): CompleteDepGraph = {
    val vertices = rules.flatMap(rule => rule.allAtoms.map(Vertex(_)))
    val edges = (for (rule <- rules) yield {
      for (subgoal <- rule.subgoals) yield
        Edge(Vertex(rule.head), Vertex(subgoal))
    }).flatten

    new CompleteDepGraph(vertices, edges)
  }
}
