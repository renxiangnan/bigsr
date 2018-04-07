package org.bigsr.fwk.program.graph

import org.bigsr.fwk.program.formula.atom.ExtendedAtom

/**
  * @author xiangnan ren
  */
class Vertex(atom: ExtendedAtom) {
  def getPredicate = atom.getPredicate

  override def toString: String = atom.getPredicate

  override def hashCode(): Int = this.atom.hashCode()

  override def equals(obj: scala.Any): Boolean =
    if (!obj.isInstanceOf[Vertex]) false
    else obj match {
      case _vertex: Vertex =>
        val _atom = _vertex.getStdAtom
        getStdAtom.getPredicate == _atom.getPredicate

      case _ => false
    }

  def getStdAtom = atom.getAtom

}

object Vertex {
  def apply(atom: ExtendedAtom): Vertex = new Vertex(atom)
}
