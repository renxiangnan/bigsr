package org.bigsr.engine.core.flink.flinkeval.evaluator.op

import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.bigsr.engine.core.flink.flinkeval.conf.FlinkTimeWindowParam
import org.bigsr.fwk.common.Fact

/**
  * @author xiangnan ren
  */
private[op]
object FlinkOpHelper {

  @throws
  def windowlessJoin(leftStream: DataStream[Fact],
                     rightStream: DataStream[Fact],
                     keyIndices: (Int, Int)): DataStream[Fact] = {
    // TODO
    throw new UnsupportedOperationException("Windowless join is not supported yet")
  }

  def timedWindowJoin(leftStream: DataStream[Fact],
                      rightStream: DataStream[Fact],
                      keyIndices: (Int, Int),
                      windowParam: FlinkTimeWindowParam): DataStream[Fact] = {
    val window =
      if (windowParam.range == windowParam.slide)
        TumblingEventTimeWindows.of(windowParam.range)
      else SlidingEventTimeWindows.of(windowParam.range, windowParam.slide)


    leftStream.join(rightStream).where(_ (keyIndices._1)).equalTo(_ (keyIndices._2)).
      window(window).apply { (left: Fact, right: Fact) =>
      left ++ right.patch(keyIndices._2, Nil, 1)
    }
  }

  def distinct(inputStream: DataStream[Fact],
               windowParam: FlinkTimeWindowParam): DataStream[Fact] = {
    val win =
      if (windowParam.range == windowParam.slide)
        TumblingEventTimeWindows.of(windowParam.range)
      else SlidingEventTimeWindows.of(windowParam.range, windowParam.slide)

    inputStream.keyBy(fact => fact).
      window(win).apply(new DistinctWindowFunction)
  }

  private class DistinctWindowFunction()
    extends WindowFunction[Fact, Fact, Fact, TimeWindow] {
    override def apply(key: Fact,
                       window: TimeWindow,
                       input: Iterable[Fact],
                       out: Collector[Fact]): Unit = {
      input.toSet.foreach(out.collect)
    }
  }

}
