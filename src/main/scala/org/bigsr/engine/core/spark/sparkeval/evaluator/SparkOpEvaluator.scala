package org.bigsr.engine.core.spark.sparkeval.evaluator

import org.apache.spark.rdd.RDD
import org.bigsr.engine.core.spark.common.SparkRelationMap
import org.bigsr.engine.core.spark.sparkeval.evaluator.op.SparkOpConversion
import org.bigsr.fwk.common.Fact
import org.bigsr.fwk.program.compiler.algebra._

import scala.collection.mutable

/**
  * @author xiangnan ren
  */
private[spark]
case class SparkOpEvaluator(root: Op,
                            input: SparkRelationMap)
  extends OpVisitorBase {
  private val stack = new mutable.Stack[RDD[Fact]]
  private val walker = OpWalker(this)

  def eval: RDD[Fact] = {
    walker.walkBottomUp(root)
    stack.pop()
  }

  override def visit(op: Op): Unit = {
    import SparkOpConversion._

    op match {
      case _op: Selection =>
        val relation = input(_op.relationName)
        stack.push(_op.eval(relation.data))

      case _op: Projection =>
        stack.push(_op.eval(stack.pop))

      case _op: Join =>
        val rightRelation = stack.pop
        val leftRelation = stack.pop
        stack.push(_op.eval(leftRelation, rightRelation))

      case _op: Union =>
        val rightRelation = stack.pop
        val leftRelation = stack.pop
        stack.push(_op.eval(leftRelation, rightRelation))
    }
  }
}
