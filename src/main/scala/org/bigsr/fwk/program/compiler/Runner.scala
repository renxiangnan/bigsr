package org.bigsr.fwk.program.compiler

import org.bigsr.fwk.program.Program

/**
  * @author xiangnan ren
  */

abstract class Runner(@transient val program: Program) {
  type T

  def initLogicalPlan: Vector[T]

}
