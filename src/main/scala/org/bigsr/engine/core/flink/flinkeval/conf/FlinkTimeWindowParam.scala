package org.bigsr.engine.core.flink.flinkeval.conf

import org.apache.flink.streaming.api.windowing.time.Time
import org.bigsr.fwk.program.formula.TimeWindowParam

/**
  * @author xiangnan ren
  */
class FlinkTimeWindowParam(rangeSize: Long,
                           slideSize: Long)
  extends TimeWindowParam {
  type T = Time

  override def range: Time = Time.seconds(rangeSize)

  override def slide: Time = Time.seconds(slideSize)

  override def toString: String = s"Range: $rangeSize, Slide $slideSize"

}

object FlinkTimeWindowParam {
  def apply(rangeSize: Long,
            slideSize: Long): FlinkTimeWindowParam = {
    new FlinkTimeWindowParam(rangeSize, slideSize)
  }
}