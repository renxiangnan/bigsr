package org.bigsr.fwk.program.compiler.algebra

/**
  * @author xiangnan ren
  */
object AlgebraPrettyPrinter extends OpVisitorBase {

  def print(op: Op): Unit = OpWalker(this).walkBottomUp(op)

  override def visit(op: Op): Unit = op match {
    case opAlias: Selection => println(s"Selection { ${opAlias.relationName} }")
    case opAlias: Projection => println(s"Projection { ${opAlias.relationName} }")
    case opAlias: Op2 => println(s"{ ${opAlias.relationName}")
  }
}
