package org.bigsr.fwk.program.graph

import org.bigsr.fwk.program.formula.atom.Atom

/**
  * @author xiangnan ren
  */
abstract class DependencyGraph {
  type G <: DependencyGraph
  protected val simplifyVertex: Vertex => Vertex =
    (vertex) => {
      Vertex(Atom(vertex.getPredicate))
    }

  def reversedGraph: G

  def vertices: Set[Vertex]

  def edges: Set[Edge]

  def simplify(): G

}



