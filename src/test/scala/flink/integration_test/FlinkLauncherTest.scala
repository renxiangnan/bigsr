package flink.integration_test

import flink.common.CollectionSrcFun
import flink.SharedFlinkEnv
import org.apache.flink.streaming.api.scala._
import org.bigsr.engine.core.flink.flinkeval.evaluator.FlinkRunner
import org.bigsr.engine.core.flink.stream.FlinkStreamUtils
import org.bigsr.program_examples.ExampleProgramBuilder

/**
  * @author xiangnan ren
  */
object FlinkLauncherTest extends SharedFlinkEnv {
  def main(args: Array[String]): Unit = {
    val program = ExampleProgramBuilder.apply("NR_Waves_2", 1, 1)
    val source = env.addSource(new CollectionSrcFun).
      map { s =>
        val seq = s.split(" ", 4)
        Seq(seq(0), seq(1), seq(2))
      }
    val flinkRunner = FlinkRunner(program)
    val inputStreamMap = FlinkStreamUtils.convertSeqStream(program, source)
    val outputStreamMap = flinkRunner.launch(inputStreamMap)
    val res = FlinkStreamUtils.latencyMonitor(outputStreamMap("<http://big-sr/result>"))
    res.writeAsText("/Users/xiangnanren/IDEAWorkspace/stream-reasoning/bigsr/src/main/resources/res.txt").setParallelism(1)

    env.execute()
  }

}


