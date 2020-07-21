package org.bigsr.engine.core.spark

import org.bigsr.engine.core.spark.sparkeval.conf.{SparkRunnerEnv, SparkSingleton, SparkTimeWindowParam}
import org.bigsr.engine.core.spark.stream.SparkStreamUtils
import org.bigsr.fwk.common.Utils
import org.bigsr.fwk.stream.{KafkaConsumerParam, MsgRDFTriple}
import org.bigsr.program_examples.ExampleProgramBuilder

/**
  * @author xiangnan ren
  */
object SparkLauncher {

  def main(args: Array[String]): Unit = {
    if (args.length < 13) {
      System.err.println(
        s"""
           |Usage:
           |
           | <localMode>           local mode execution;
           | <brokerAddr>          kafka broker address;
           | <zkConnection>        zookeeper address;
           | <repartition>         repartition input DStream into n partition;
           | <numPartition>        default parallelism level;
           | <shuffledPartition>   number of partition after join;
           | <checkpointThreshold> max iteration to trigger local checkpoint;
           | <concurrentJobs>      concurrent spark streaming jobs;
           | <range>               Spark Streaming window size (second);
           | <slide>               Spark Streaming window sliding size (second)
           | <batch>               Spark Streaming batch size (second)
           | <query>               pre-defined query id;
           | <idb>                 output idb predicate;
           |
        """.stripMargin)
    }

    val Array(
    localMode, brokerAddr, zkConnection, repartition,
    numPartition, shuffledPartition, checkpointThreshold,
    concurrentJobs, range, slide, batch, query, idb) = args

    val sparkSingleton =
      SparkSingleton.init(
        localMode.toBoolean,
        numPartition.toInt,
        shuffledPartition.toInt,
        concurrentJobs.toInt,
        checkpointThreshold.toInt)

    val program = ExampleProgramBuilder(query)
    val env = new SparkRunnerEnv(
      sparkSingleton.conf,
      SparkTimeWindowParam(range.toInt, slide.toInt, batch.toInt)).
      addCheckpointDir("big-sr-spark-checkpoint")

    val kafkaParam = KafkaConsumerParam(
      brokerAddr, zkConnection,
      "consumer-group-test",
      None, MsgRDFTriple.topic)

    val runner = env.addRunner(program)
    val stream = env.addStream(kafkaParam, repartition.toInt)

    stream.foreachRDD { rdd =>
      println(s"Input #triples: ${rdd.count()}")
      Utils.displayExecutionTime {
      runner.launch(
        SparkStreamUtils.convertTripleStream(program, rdd),
        SparkSingleton.spark, idb)
      }

      rdd.unpersist(true)
    }

    env.streamingCtx.start()
    env.streamingCtx.awaitTermination()
    env.streamingCtx.stop()
  }

}
