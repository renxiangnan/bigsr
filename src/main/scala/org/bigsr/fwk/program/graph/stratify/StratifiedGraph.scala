package org.bigsr.fwk.program.graph.stratify

import org.bigsr.fwk.program.graph.{DependencyGraph, Vertex}

/**
  * @author xiangnan ren
  */
case class StratifiedGraph(graph: DependencyGraph,
                           nestedVerticesSet: Set[Set[Vertex]],
                           label: Map[Vertex, Int]) {
}
