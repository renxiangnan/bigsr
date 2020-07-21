package org.bigsr.fwk.program.graph

/**
  * @author xiangnan ren
  */
class Edge(val src: Vertex,
           val dst: Vertex,
           val label: String) {
  override final def hashCode(): Int =
    this.src.hashCode() + this.dst.hashCode()

  def reversed: Edge = Edge(this.dst, this.src, this.label)

  override def toString: String = s"($src -[${this.label}]-> $dst)"

  override def equals(obj: scala.Any): Boolean = {
    if (!obj.isInstanceOf[Edge]) false
    else {
      val edge = obj.asInstanceOf[Edge]
      this.src.equals(edge.src) && this.dst.equals(edge.dst)
    }
  }
}

object Edge {
  def apply(src: Vertex,
            dst: Vertex,
            label: String): Edge =
    new Edge(src, dst, label)

  def apply(src: Vertex,
            dst: Vertex): Edge = {
    val label = if (dst.getStdAtom.isPositive) ""
    else "-"
    new Edge(src, dst, label)
  }
}