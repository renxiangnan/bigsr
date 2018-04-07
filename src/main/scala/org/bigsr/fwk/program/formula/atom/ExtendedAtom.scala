package org.bigsr.fwk.program.formula.atom

import org.bigsr.fwk.program.formula.Formula

/**
  * @author xiangnan ren
  */

/**
  * A extended atom refers to a standard atom which may involve
  * window function.
  */
trait ExtendedAtom extends Formula {

  def getAtom: Atom

  def getPredicate: String

  def variables: Set[String]

}
