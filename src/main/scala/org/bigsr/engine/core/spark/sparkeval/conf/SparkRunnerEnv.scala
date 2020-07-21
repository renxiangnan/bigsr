package org.bigsr.engine.core.spark.sparkeval.conf

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.bigsr.engine.core.spark.sparkeval.evaluator.SparkProgramRunner
import org.bigsr.engine.core.spark.stream.StreamDeserializer
import org.bigsr.fwk.program.Program
import org.bigsr.fwk.rdf.RDFTriple
import org.bigsr.fwk.stream.KafkaConsumerParam

/**
  * @author xiangnan ren
  */
class SparkRunnerEnv(conf: SparkConf,
                     winParam: SparkTimeWindowParam) {
  // The time unit is set as second by default
  val streamingCtx =
    new StreamingContext(conf, winParam.batch)

  def addRunner(program: Program): SparkProgramRunner = {
    new SparkProgramRunner(program)
  }

  def addStream(param: KafkaConsumerParam,
                repartition: Int): DStream[RDFTriple] = {
    def windowing(input: InputDStream[(String, RDFTriple)]):
    DStream[RDFTriple] = {
      val windowed =
        if (winParam.batch != winParam.range)
          input.window(winParam.range, winParam.slide)
        else input
      windowed.mapPartitions(for (i <- _) yield i._2)
    }

    val stream = KafkaUtils.
      createDirectStream[
      String,
      RDFTriple,
      StringDecoder,
      StreamDeserializer[RDFTriple]](
      streamingCtx,
      param.kafkaParams,
      param.topicsSet)

    if (repartition > 0)
      windowing(stream).repartition(repartition)
    else windowing(stream)
  }

  def addCheckpointDir(path: String): SparkRunnerEnv = {
    SparkSingleton.spark.sparkContext.setCheckpointDir(path)
    this
  }

}
