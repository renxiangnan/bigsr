package org.bigsr.fwk.program.compiler.algebra

/**
  * @author xiangnan ren
  */
trait OpVisitor {
  def visit(op: Op): Unit
}

abstract class OpVisitorBase extends OpVisitor {
  @throws
  override def visit(op: Op): Unit = op match {
    case _: Selection => throw new UnsupportedOperationException("Selection is not supported.")
    case _: Projection => throw new UnsupportedOperationException("Projection is not supported.")
    case _: Join => throw new UnsupportedOperationException("InnerJoin is not supported.")
    case _: Union => throw new UnsupportedOperationException("Union is not supported.")
  }
}
