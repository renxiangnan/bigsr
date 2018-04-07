package org.bigsr.fwk.program.graph.stratify

import org.bigsr.fwk.program.graph.Edge

/**
  * @author xiangnan ren
  */
case class SCCEdge(src: SCCVertex,
                   dst: SCCVertex,
                   edges: Set[Edge]) {
  override def toString = s"{" +
    s"src_sscVertex${src.toString}, " +
    s"dst_sscVertex${dst.toString}, [$edges]}"

}
