package org.bigsr.engine.core.spark.sparkeval.conf

import org.apache.spark.streaming.{Duration, Seconds}
import org.bigsr.fwk.program.formula.TimeWindowParam

/**
  * @author xiangnan ren
  */
private[spark]
class SparkTimeWindowParam(rangeSize: Long,
                           slideSize: Long,
                           batchSize: Long) extends TimeWindowParam {
  override type T = Duration

  override def range: Duration = Seconds(rangeSize)

  override def slide: Duration = Seconds(slideSize)

  def batch: Duration = Seconds(batchSize)

}

object SparkTimeWindowParam {
  def apply(rangeSize: Long,
            slideSize: Long,
            batchSize: Long): SparkTimeWindowParam = {
    new SparkTimeWindowParam(rangeSize, slideSize, batchSize)
  }
}
