package org.bigsr.fwk.program.graph

import org.bigsr.fwk.program.formula.atom.ExtendedAtom
import org.bigsr.fwk.program.rule.Rule

import scala.collection.mutable

/**
  * @author xiangnan ren
  */
class StdDepGraph(override val vertices: Set[Vertex],
                  override val edges: Set[Edge])
  extends DependencyGraph {
  override type G = StdDepGraph

  override def simplify(): StdDepGraph = {
    val intensionalPredicates = mutable.Set[Vertex]()
    edges.foreach { edge =>
      intensionalPredicates.add(edge.src)
      intensionalPredicates.add(simplifyVertex(edge.dst))
    }
    val newEdges = {
      for (edge <- edges
           if intensionalPredicates.contains(edge.src) &&
             intensionalPredicates.contains(edge.dst))
        yield edge
    }.map(e => Edge(
      simplifyVertex(e.src),
      simplifyVertex(e.dst), e.label))

    new StdDepGraph(intensionalPredicates.toSet, newEdges)
  }

  override def reversedGraph: StdDepGraph = {
    val reversedEdges = this.edges.map(_.reversed)
    new StdDepGraph(this.vertices, reversedEdges)
  }
}

object StdDepGraph {
  def apply(rules: Set[Rule]): StdDepGraph = {
    val intensionalAtoms = createAtomSet(rules)._1
    val vertices = intensionalAtoms.map(new Vertex(_))
    val edges =
      (for (rule <- rules
            if vertices.contains(Vertex(rule.head)))
        yield {
          val intersection = createCommonVertices(rule.subgoals, intensionalAtoms)
          for (vertex <- intersection)
            yield Edge(Vertex(rule.head), vertex, "")
        }).flatten

    new StdDepGraph(vertices, edges)
  }

  def createAtomSet(rules: Set[Rule]):
  (Set[ExtendedAtom], Set[ExtendedAtom]) = {
    val allAtoms = mutable.Set[ExtendedAtom]()
    val intensionalAtoms = mutable.Set[ExtendedAtom]()

    rules.foreach { rule =>
      intensionalAtoms.add(rule.head)
      (allAtoms ++= rule.subgoals).add(rule.head)
    }

    (intensionalAtoms.toSet,
      allAtoms.diff(intensionalAtoms).toSet)
  }

  private def createCommonVertices(subgoals: Set[ExtendedAtom],
                                   intensionalAtoms: Set[ExtendedAtom]): Set[Vertex] = {
    (for (subgoal <- subgoals) yield {
      for (ia <- intensionalAtoms
           if ia.getPredicate == subgoal.getPredicate) yield {
        Vertex(subgoal)
      }
    }).flatten
  }

}