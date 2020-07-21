package org.bigsr.engine.core.flink.stream

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.fs.Clock
import org.apache.flink.streaming.connectors.fs.bucketing.Bucketer
import org.apache.flink.util.Collector
import org.apache.hadoop.fs.Path
import org.apache.spark.annotation.Experimental
import org.bigsr.engine.core.flink.common.{FlinkEDBMap, FlinkStreamMap}
import org.bigsr.fwk.common.Fact
import org.bigsr.fwk.program.Program
import org.bigsr.fwk.rdf.RDFTriple

/**
  * @author xiangnan ren
  */
object FlinkStreamUtils {
  def convertTripleStream(program: Program,
                          inputStream: DataStream[RDFTriple],
                          withPredicate: Boolean = false): FlinkEDBMap = {
    program.getEDBPredicates.map(predicate => predicate -> {
      if (!withPredicate) {
        inputStream.filter(t => t.predicate == predicate).
          map(t => t.asFact)
      } else {
        inputStream.filter(t => t.predicate == predicate).
          map(t => t.asTripleFact)
      }
    }).toMap
  }

  def convertSeqStream(program: Program,
                       inputStream: DataStream[Seq[String]],
                       withPredicate: Boolean = false): FlinkEDBMap = {
    program.getEDBPredicates.map(predicate => predicate -> {
      if (withPredicate) {
        inputStream.filter(t => t(1) == predicate)
      } else {
        inputStream.filter(t => t(1) == predicate).
          map(t => t.patch(1, Nil, 1))
      }
    }).toMap
  }

  def resultPrettyPrint(predicate: String,
                        streamMap: FlinkStreamMap): DataStream[Fact] = {
    streamMap(predicate).map { fact =>
      println(s"predicate: $predicate, fact: ${fact.mkString(",")}"); fact
    }
  }

  def latencyMonitor(input: DataStream[Fact],
                     size: Long = 10L): DataStream[String] = {
    input.process(new CollectExecTimeFun).keyBy(0).
      window(TumblingEventTimeWindows.of(Time.seconds(size))).
      reduce { (r1, r2) =>
        val minInput = Math.min(r1._1, r2._1)
        val maxOutput = Math.max(r1._2, r2._2)
        (minInput, maxOutput)
      }.map(t => s"${t._2 - t._1}")
  }

}

private[stream]
class CollectExecTimeFun extends ProcessFunction[Fact, (Long, Long)] {
  override def processElement(value: Fact,
                              context: ProcessFunction[Fact, (Long, Long)]#Context,
                              out: Collector[(Long, Long)]): Unit = {
    out.collect((context.timestamp(), System.currentTimeMillis()))
  }
}

private[stream]
class DisplayLatencyFun extends ProcessFunction[Fact, Unit] {
  override def processElement(value: Fact,
                              context: ProcessFunction[Fact, Unit]#Context,
                              out: Collector[Unit]): Unit = {
    val executionTime = System.currentTimeMillis() - context.timestamp()
    println(s"The latency of current record (ms): $executionTime")
  }
}

@Experimental private[stream]
class S3Bucketer(val fileNamePath: String) extends Bucketer[(Long, Long)] {
  override def getBucketPath(clock: Clock,
                             basePath: Path,
                             element: (Long, Long)): Path = {
    new Path(fileNamePath)
  }
}