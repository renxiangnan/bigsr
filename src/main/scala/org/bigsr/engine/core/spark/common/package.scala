package org.bigsr.engine.core.spark

import org.bigsr.engine.core.spark.sparkeval.evaluator.SparkRelation

import scala.collection.mutable

/**
  * @author xiangnan ren
  */

package object common {
  private[spark] type SparkEDBMap = Map[String, SparkRelation]
  private[spark] type SparkRelationMap = mutable.Map[String, SparkRelation]

}
