package org.bigsr.fwk.program.formula

/**
  * @author xiangnan ren
  */
class AndFormula(override val leftChild: Formula,
                 override val rightChild: Formula) extends
  Formula2(leftChild, rightChild) {

}
