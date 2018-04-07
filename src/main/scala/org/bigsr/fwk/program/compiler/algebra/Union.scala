package org.bigsr.fwk.program.compiler.algebra

import org.bigsr.fwk.program.RelationSchemaException

/**
  * @author xiangnan ren
  */

class Union(leftOp: Op,
            rightOp: Op)
  extends Op2(leftOp, rightOp) {

  override val schema =
    if ((leftOp.schema corresponds rightOp.schema) {
      _ == _
    }) leftOp.schema
    else throw RelationSchemaException("" +
      "The input should not have the same schema for union:" +
      s"left schema: ${leftOp.schema}, right schema: ${rightOp.schema}")
  override val relationName = s"UNION { ${schema.mkString(", ")} }"

  override def visit(opVisitor: OpVisitor): Unit =
    opVisitor.visit(this)
}

object Union {
  def apply(leftOp: Op,
            rightOp: Op): Union =
    new Union(leftOp, rightOp)
}
