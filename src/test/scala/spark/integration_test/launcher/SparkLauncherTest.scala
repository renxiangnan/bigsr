package spark.integration_test.launcher

import org.bigsr.engine.core.spark.sparkeval.conf.{SparkRunnerEnv, SparkSingleton, SparkTimeWindowParam}
import org.bigsr.engine.core.spark.stream.SparkStreamUtils
import org.bigsr.fwk.stream.{KafkaConsumerParam, MsgRDFTriple}
import org.bigsr.program_examples.ExampleProgramBuilder

/**
  * @author xiangnan ren
  */
object SparkLauncherTest {
  def main(args: Array[String]): Unit = {
    val program = ExampleProgramBuilder("NR_Waves_1")
    val sparkSingleton = SparkSingleton.init(true: Boolean, 1, 1, 1)

    val env = new SparkRunnerEnv(sparkSingleton.conf, SparkTimeWindowParam(10, 10, 10)).
      addCheckpointDir("big-sr-spark-checkpoint")

    val kafkaParam = KafkaConsumerParam(
      "localhost:9092", "localhost:2181",
      "consumer-group-test", None, MsgRDFTriple.topic)

    val runner = env.addRunner(program)
    val stream = env.addStream(kafkaParam, -1)

    stream.foreachRDD { rdd =>
      runner.launch(
        SparkStreamUtils.convertTripleStream(program, rdd),
        SparkSingleton.spark, "<http://big-sr/result>")
    }

    env.streamingCtx.start()
    env.streamingCtx.awaitTermination()
    env.streamingCtx.stop()
  }
}
