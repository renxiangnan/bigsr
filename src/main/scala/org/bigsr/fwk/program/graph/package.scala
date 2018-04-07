package org.bigsr.fwk.program

/**
  * @author xiangnan ren
  */
package object graph {
  private[graph] type LeafVertices = Map[Int, Set[Vertex]]
  private[graph] type InnerVertices = Map[Int, Set[Vertex]]
  private[graph] type UpstreamIdDct = Map[Int, Set[Int]] // Current SCC ID -> Upstream SCC ID
  private[graph] type DownstreamIdDct = Map[Int, Set[Int]] // Current SCC ID -> Downstream SCC ID

  case class DependencyIdDct(upstreamIdDct: UpstreamIdDct,
                             downstreamIdDct: DownstreamIdDct)

}
