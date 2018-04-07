package org.bigsr.engine.core.spark.sparkeval.evaluator

import org.apache.spark.rdd.RDD
import org.bigsr.fwk.common.Fact

/**
  * @author xiangnan ren
  */

class SparkRelation(val data: RDD[Fact],
                    var isEmpty: Boolean = false) {
  def show(numRows: Int = 20): Unit =
    data.take(numRows).foreach { fact =>
      println(s"(${fact.mkString(", ")})")
    }

  def clean(): Unit = {
    data.unpersist(true)
  }
}

object SparkRelation {
  def apply(data: RDD[Fact]): SparkRelation = {
    new SparkRelation(data)
  }
}

