package org.bigsr.fwk.program.graph

/**
  * @author xiangnan ren
  */
sealed trait DependencyConstraint

case object Negation extends DependencyConstraint {
  override def toString = "-"
}

