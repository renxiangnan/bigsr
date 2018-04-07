package org.bigsr.engine.core.flink.flinkeval.evaluator

import org.apache.flink.streaming.api.scala.DataStream
import org.bigsr.engine.core.flink.common.FlinkStreamMap
import org.bigsr.engine.core.flink.flinkeval.evaluator.op.FlinkOpConversion
import org.bigsr.fwk.common.Fact
import org.bigsr.fwk.program.compiler.algebra._

import scala.collection.mutable

/**
  * @author xiangnan ren
  */
private[evaluator]
case class FlinkOpEvaluator(root: Op,
                            input: FlinkStreamMap)
  extends OpVisitorBase {
  private val stack = new mutable.Stack[DataStream[Fact]]
  private val walker = OpWalker(this)

  def eval: DataStream[Fact] = {
    walker.walkBottomUp(root)
    stack.pop()
  }

  override def visit(op: Op): Unit = {
    import FlinkOpConversion._
    op match {
      case _op: Selection =>
        val stream = input(_op.relationName)
        stack.push(_op.eval(stream))

      case _op: Projection =>
        stack.push(_op.eval(stack.pop))

      case _op: Union =>
        val rightStream = stack.pop()
        val leftStream = stack.pop()
        stack.push(_op.eval(leftStream, rightStream))

      case _op: Join =>
        val rightStream = stack.pop()
        val leftStream = stack.pop()
        stack.push(_op.eval(leftStream, rightStream))
    }
  }
}
