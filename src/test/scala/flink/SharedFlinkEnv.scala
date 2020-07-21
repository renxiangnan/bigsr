package flink

import org.bigsr.engine.core.flink.flinkeval.conf.FlinkEnv

/**
  * @author xiangnan ren
  */
trait SharedFlinkEnv {
  FlinkEnv.setParaLevelConfig("envParallelism", "1")
  val env = FlinkEnv.getInstance()
}
