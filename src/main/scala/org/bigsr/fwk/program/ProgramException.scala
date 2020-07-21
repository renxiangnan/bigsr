package org.bigsr.fwk.program

/**
  * @author xiangnan ren
  */

case class NotStratifiedException(msg: String) extends Exception(msg)

case class NonUniformException(msg: String) extends Exception(msg)

case class RelationSchemaException(msg: String) extends Exception(msg)

case class EmptyJoinKeyException(msg: String) extends Exception(msg)

case class InvalidProjectionSchema(msg: String) extends Exception(msg)
