package org.bigsr.fwk.program.graph.stratify

import org.bigsr.fwk.program.graph.{DependencyGraph, Edge, Vertex}

/**
  * @author xiangnan ren
  */
class SCCVertex(val id: Int,
                val nestedVertices: Set[Vertex],
                val nestedEdges: Set[Edge]) {
  override def toString: String = s"($id,$nestedVertices)"

}

object SCCVertex {
  def apply(id: Int,
            nestedVertices: Set[Vertex],
            graph: DependencyGraph): SCCVertex = {
    val nestedEdges =
      for (e <- graph.edges
           if (nestedVertices.exists(v => v.equals(e.src)) &&
             nestedVertices.exists(v => v.equals(e.dst))) ||
             (nestedVertices.exists(v => v.equals(e.dst)) &&
               nestedVertices.exists(v => v.equals(e.src))))
        yield e

    new SCCVertex(id, nestedVertices, nestedEdges)
  }

}
