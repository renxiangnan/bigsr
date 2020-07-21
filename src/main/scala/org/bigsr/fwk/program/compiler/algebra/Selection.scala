package org.bigsr.fwk.program.compiler.algebra

import org.bigsr.fwk.program.formula.atom.{Atom, Term}


/**
  * @author xiangnan ren
  */

class Selection(val relationName: String,
                val schema: Seq[Term]) extends Op0 {
  protected val filterInd =
    for (i <- schema.indices
         if schema(i).isConstant) yield i

  override def visit(opVisitor: OpVisitor): Unit = {
    opVisitor.visit(this)
  }
}

object Selection {
  def apply(relationName: String,
            atom: Atom): Selection = {
    new Selection(relationName, atom.schema)
  }

  def apply(atom: Atom): Selection = {
    new Selection(atom.getPredicate, atom.schema)
  }
}


