package org.bigsr.engine.core.spark.sparkeval.evaluator.op

import org.bigsr.fwk.program.compiler.algebra.{Join, Projection, Selection, Union}

/**
  * @author xiangnan ren
  */

private[spark]
object SparkOpConversion {
  implicit def toSparkProjection(op: Projection): SparkProjection = {
    new SparkProjection(op.relationName, op.schema, op.subOp)
  }

  implicit def toSparkSelection(op: Selection): SparkSelection = {
    new SparkSelection(op.relationName, op.schema)
  }

  implicit def toSparkJoin(op: Join): SparkJoin = {
    new SparkJoin(op.leftOp, op.rightOp)
  }

  implicit def toSparkUnion(op: Union): SparkUnion = {
    new SparkUnion(op.leftOp, op.rightOp)
  }
}
