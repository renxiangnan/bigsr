package org.bigsr.fwk.program.formula


/**
  * @author xiangnan ren
  */
trait TimeWindowParam extends java.io.Serializable {
  type T

  def range: T

  def slide: T

  def toString: String

}




