package org.bigsr.fwk.program.compiler.algebra

import org.bigsr.fwk.program.formula.atom.Term

abstract class Op extends Serializable {
  val relationName: String
  val schema: Seq[Term]
  val leaves: Set[String]

  def visit(opVisitor: OpVisitor): Unit
}

abstract class Op0 extends Op {
  override val leaves = Set(relationName)
}

abstract class Op1(val subOp: Op) extends Op {
  override val leaves = subOp.leaves
}

abstract class Op2(val leftOp: Op,
                   val rightOp: Op) extends Op {
  override val leaves = leftOp.leaves ++ rightOp.leaves

}
