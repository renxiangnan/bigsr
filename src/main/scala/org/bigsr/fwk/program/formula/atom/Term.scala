package org.bigsr.fwk.program.formula.atom

/**
  * @author xiangnan ren
  */
sealed abstract class Term {
  override def toString: String

  final def isConstant: Boolean = this match {
    case _: Constant => true
    case _: Variable => false
  }

}

case class Constant(constant: String) extends Term {
  override def toString: String = constant
}

case class Variable(variable: String) extends Term {
  override def toString: String = variable
}

object Term {
  def apply(str: String): Term = {
    if (str(0).isUpper || str.startsWith("?")) Variable(str)
    else Constant(str)
  }
}

