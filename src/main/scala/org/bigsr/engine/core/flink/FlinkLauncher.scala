package org.bigsr.engine.core.flink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer09, FlinkKafkaProducer09}
import org.bigsr.engine.core.flink.flinkeval.conf.{FlinkEnv, RDFTripleSchema}
import org.bigsr.engine.core.flink.flinkeval.evaluator.op.{FlinkJoin, FlinkProjection, FlinkSelection, FlinkUnion}
import org.bigsr.engine.core.flink.stream.FlinkStreamUtils
import org.bigsr.fwk.rdf.RDFTriple
import org.bigsr.fwk.stream.{KafkaConsumerParam, MsgRDFTriple}
import org.bigsr.program_examples.ExampleProgramBuilder

/**
  * @author xiangnan ren
  */
object FlinkLauncher {
  def main(args: Array[String]): Unit = {
    if (args.length < 14) {
      System.err.println(
        s"""
           |Usage:
           |
           | <brokerAddr>             kafka broker address;
           | <zkConnection>           zookeeper address;
           | <numPartition>           number of topic partition;
           | <envParallelism>         Global parallelism level;
           | <selectionParallelism>   parallelism level of selection;
           | <projectionParallelism>  parallelism level of projection;
           | <joinParallelism>        parallelism level of join;
           | <unionParallelism>       parallelism level of union;
           | <range>                  Flink window size (second);
           | <slide>                  Flink window sliding size (second)
           | <query>                  pre-defined query id;
           | <idb>                    output idb predicate;
           | <outputBroker>           output kafka broker address;
           | <outputTopic>            output kafka topic;
           |
        """.stripMargin)
    }

    val Array(brokerAddr, zkConnection, numPartition, envParallelism,
    selectionParallelism, projectionParallelism, joinParallelism, unionParallelism,
    range, slide, query, idb, outputBroker, outputTopic) = args

    val env = FlinkEnv.setParaLevelConfig("envParallelism", envParallelism).
      setParaLevelConfig(classOf[FlinkSelection].getCanonicalName, selectionParallelism).
      setParaLevelConfig(classOf[FlinkProjection].getCanonicalName, projectionParallelism).
      setParaLevelConfig(classOf[FlinkJoin].getCanonicalName, joinParallelism).
      setParaLevelConfig(classOf[FlinkUnion].getCanonicalName, unionParallelism).
      getInstance()
    val program = ExampleProgramBuilder.apply(query, range.toLong, slide.toLong)
    val runner = FlinkEnv.addRunner(program)
    val kafkaProperties = KafkaConsumerParam(brokerAddr, zkConnection,
      "consumer-group-test", None, MsgRDFTriple.topic).asFlinkProperties()
    val kafkaOutputProducer = new FlinkKafkaProducer09[String](
      outputBroker, outputTopic, new SimpleStringSchema)

    val stream = env.addSource(
      new FlinkKafkaConsumer09[RDFTriple](
        MsgRDFTriple.topic,
        new RDFTripleSchema(MsgRDFTriple.topic),
        kafkaProperties)).setParallelism(numPartition.toInt)

    val inputStreamMap = FlinkStreamUtils.convertTripleStream(program, stream)
    val outputStreamMap = runner.launch(inputStreamMap)

    FlinkStreamUtils.latencyMonitor(outputStreamMap(idb), range.toLong).
      addSink(kafkaOutputProducer)

    env.execute()
  }
}
