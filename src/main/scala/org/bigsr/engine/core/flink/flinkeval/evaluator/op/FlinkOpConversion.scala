package org.bigsr.engine.core.flink.flinkeval.evaluator.op

import org.bigsr.fwk.program.compiler.algebra.{Join, Projection, Selection, Union}

/**
  * @author xiangnan ren
  */
private[flink]
object FlinkOpConversion {
  implicit def toFlinkProjection(op: Projection): FlinkProjection = {
    new FlinkProjection(op.relationName, op.schema, op.subOp, op.windowParam)
  }

  implicit def toFlinkSelection(op: Selection): FlinkSelection = {
    new FlinkSelection(op.relationName, op.schema)
  }

  implicit def toFlinkJoin(op: Join): FlinkJoin = {
    new FlinkJoin(op.leftOp, op.rightOp, op.windowParam)
  }

  implicit def toFlinkUnion(op: Union): FlinkUnion = {
    new FlinkUnion(op.leftOp, op.rightOp)
  }

}
