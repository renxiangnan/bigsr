package org.bigsr.fwk.program.graph.stratify

import org.bigsr.fwk.program.graph._

/**
  * @author xiangnan ren
  */

class DAG(val graph: DependencyGraph,
          val sccVertices: Set[SCCVertex],
          val sccEdges: Set[SCCEdge]) {
  lazy val reversedGraph = graph.reversedGraph
  lazy val dct = DependencyIdDct(upstreamSCCId, downstreamSCCId)

  def createLeafVertex(): LeafVertices = {
    val vertices: (Set[Vertex]) => Set[Vertex] =
      (vs) => for (v <- vs
                   if !reversedGraph.edges.
                     exists(e => e.dst.equals(v))) yield v

    (for (sccVertex <- sccVertices;
          vs = vertices(sccVertex.nestedVertices)
          if vs.nonEmpty) yield sccVertex.id -> vs).toMap
  }

  def isLinear: Boolean = {
    !sccVertices.exists(v =>
      dct.upstreamIdDct.get(v.id).size > 1 ||
        dct.downstreamIdDct.get(v.id).size > 1)
  }

  private def upstreamSCCId: UpstreamIdDct = {
    for (sccVertex <- sccVertices;
         vs = sccEdges.filter(e => e.dst.id == sccVertex.id).
           map(_.src.id)
         if vs.nonEmpty) yield sccVertex.id -> vs
  }.toMap

  private def downstreamSCCId: DownstreamIdDct = {
    for (sccVertex <- sccVertices;
         vs = sccEdges.filter(e =>
           e.src.id == sccVertex.id).map(_.dst.id)
         if vs.nonEmpty) yield
      sccVertex.id -> vs
  }.toMap

}

object DAG {
  def apply(graph: DependencyGraph): DAG = {
    val sccVertices = createVertices(graph)
    val sccEdges = createEdges(graph, sccVertices)

    new DAG(graph, sccVertices, sccEdges)
  }

  private def createVertices(graph: DependencyGraph): Set[SCCVertex] = {
    val stratifiedGraph = TarjanProc.stratify(graph.reversedGraph)
    for (vs <- stratifiedGraph.nestedVerticesSet) yield {
      val vertex = vs.reduce { (v1, v2) =>
        if (stratifiedGraph.label(v1) < stratifiedGraph.label(v2))
          v1 else v2
      }
      SCCVertex(
        stratifiedGraph.label(vertex),
        vs,
        stratifiedGraph.graph)
    }
  }

  private def createEdges(graph: DependencyGraph,
                          sccVertices: Set[SCCVertex]): Set[SCCEdge] = {
    val reversedGraph = graph.reversedGraph
    val verticesSeq = sccVertices.toSeq
    val sccEdges =
      for (i <- verticesSeq.indices;
           j <- verticesSeq.indices;
           set = findAdjacent(
             verticesSeq(i),
             verticesSeq(j),
             reversedGraph.edges)
           if i != j && set.nonEmpty) yield {
        SCCEdge(verticesSeq(i), verticesSeq(j), set)
      }

    sccEdges.toSet
  }

  private def findAdjacent(vSrc: SCCVertex,
                           vDst: SCCVertex,
                           edges: Set[Edge]): Set[Edge] =
    edges.filter { e =>
      vSrc.nestedVertices.exists(v => v.equals(e.src)) &&
        vDst.nestedVertices.exists(v => v.equals(e.dst))
    }
}
