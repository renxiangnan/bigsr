package org.bigsr.engine.core.flink.flinkeval.conf

import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.bigsr.engine.core.flink.flinkeval.evaluator.FlinkRunner
import org.bigsr.engine.core.flink.flinkeval.evaluator.op.{FlinkJoin, FlinkProjection, FlinkSelection, FlinkUnion}
import org.bigsr.fwk.common.Fact
import org.bigsr.fwk.program.Program
import org.bigsr.fwk.rdf.RDFTriple

import scala.collection.mutable

/**
  * @author xiangnan ren
  */
object FlinkEnv {
  lazy val paraSettings = new mutable.HashMap[String, Int]
  val paraSettingsKey = Seq(
    "envParallelism",
    s"${classOf[FlinkSelection].getCanonicalName}",
    s"${classOf[FlinkProjection].getCanonicalName}",
    s"${classOf[FlinkJoin].getCanonicalName}",
    s"${classOf[FlinkUnion].getCanonicalName}")

  def setParaLevelConfig(key: String, value: String): FlinkEnv.type = {
    if (key == null)
      throw new NullPointerException("Null key for settings")
    if (!paraSettingsKey.contains(key))
      throw new NullPointerException("Invalid key for settings")
    if (value == null)
      throw new NullPointerException("Null value for settings")

    paraSettings.put(key, value.toInt)
    this
  }

  def setParaLevel(key: String,
                   inputStream: DataStream[Fact]): DataStream[Fact] = {
    paraSettings.get(key) match {
      case Some(level) if level > 0 => inputStream.setParallelism(level)
      case _ => inputStream
    }
  }

  def getInstance(): StreamExecutionEnvironment = {
    @transient lazy val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.registerTypeWithKryoSerializer(classOf[RDFTriple], classOf[KryoInternalSerializer])
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setStateBackend(new MemoryStateBackend(200 * 1024 * 1024))

    paraSettings.get("envParallelism") match {
      case Some(level) if level > 0 => env.setParallelism(level)
      case _ =>
    }

    env
  }

  def addRunner(program: Program): FlinkRunner = {
    FlinkRunner(program)
  }
}
