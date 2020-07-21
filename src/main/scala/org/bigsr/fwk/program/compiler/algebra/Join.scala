package org.bigsr.fwk.program.compiler.algebra

import org.bigsr.fwk.program.RelationSchemaException
import org.bigsr.fwk.program.formula.TimeWindowParam
import org.bigsr.fwk.program.formula.atom.Term

/**
  * @author xiangnan ren
  */

class Join(leftOp: Op,
           rightOp: Op,
           val windowParam: Option[TimeWindowParam] = None)
  extends Op2(leftOp, rightOp) {
  override val schema = leftOp.schema.union(rightOp.schema).distinct
  override val relationName = s"JOIN { ${schema.mkString(", ")} }"
  protected val keyIndices = getKeyIndices(leftOp.schema, rightOp.schema)

  override def visit(opVisitor: OpVisitor): Unit = {
    opVisitor.visit(this)
  }

  // Current implementation only involves single join key
  protected def getKeyIndices(left: Seq[Term],
                              right: Seq[Term]): (Int, Int) = {
    val intersect = left.intersect(right)
    val indices = for (i <- intersect) yield
      (left.indexOf(i), right.indexOf(i))

    if (indices.lengthCompare(1) == 0) indices.head
    else if (indices.isEmpty)
      throw RelationSchemaException(s"Join key must be specified, " +
        s"left [ ${leftOp.relationName} ]: ${leftOp.schema.mkString(" ")}, " +
        s"right [ ${rightOp.relationName} ]: ${rightOp.schema.mkString(" ")} ")
    else throw RelationSchemaException(
      "Multiple join keys is not support yet.")
  }
}

object Join {
  def apply(leftOp: Op,
            rightOp: Op,
            windowParam: Option[TimeWindowParam] = None): Join =
    new Join(leftOp, rightOp, windowParam)

}