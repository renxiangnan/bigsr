package org.bigsr.fwk.program.compiler.algebra

/**
  * @author xiangnan ren
  */
class OpWalker(val opVisitor: OpVisitor) extends OpVisitor {
  def walkBottomUp(op: Op): Unit =
    op.visit(new OpWalker(opVisitor))

  override def visit(op: Op): Unit = op match {
    case opAlias: Op0 => opAlias.visit(opVisitor)

    case opAlias: Op1 =>
      if (Option(opAlias.subOp).nonEmpty) opAlias.subOp.visit(this)
      opAlias.visit(opVisitor)

    case opAlias: Op2 =>
      if (Option(opAlias.leftOp).nonEmpty) opAlias.leftOp.visit(this)
      if (Option(opAlias.rightOp).nonEmpty) opAlias.rightOp.visit(this)
      opAlias.visit(opVisitor)
  }
}

object OpWalker {
  def apply(opVisitor: OpVisitor): OpWalker =
    new OpWalker(opVisitor)
}
