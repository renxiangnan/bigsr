package org.bigsr.fwk.program.compiler.algebra

import org.bigsr.fwk.program.InvalidProjectionSchema
import org.bigsr.fwk.program.formula.TimeWindowParam
import org.bigsr.fwk.program.formula.atom.Term

/**
  * @author xiangnan ren
  */

class Projection(override val relationName: String,
                 override val schema: Seq[Term],
                 subOp: Op,
                 val windowParam: Option[TimeWindowParam] = None)
  extends Op1(subOp) {

  protected val indices = getPosition

  override def visit(opVisitor: OpVisitor): Unit = {
    opVisitor.visit(this)
  }

  private def getPosition: Seq[Int] = {
    try {
      for (n <- schema) yield subOp.schema.indexOf(n)
    } catch {
      case _: Exception =>
        throw InvalidProjectionSchema("Required projection field is invalid")
    }
  }
}

object Projection {
  def apply(relationName: String,
            schema: Seq[Term],
            subOp: Op,
            windowParam: Option[TimeWindowParam] = None): Projection =
    new Projection(relationName, schema, subOp, windowParam)
}