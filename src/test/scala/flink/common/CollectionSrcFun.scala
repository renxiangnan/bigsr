package flink.common

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

/**
  * @author xiangnan ren
  */
class CollectionSrcFun extends SourceFunction[String] {
  var isRunning: Boolean = true

  override def cancel(): Unit = isRunning = false

  override def run(ctx: SourceContext[String]): Unit = {
    while (isRunning) {
      Thread.sleep(1000)
      SampleDataSource.wavesTriples.foreach(t => ctx.collect(t))
    }
  }
}
