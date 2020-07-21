package org.bigsr.engine.core.flink.flinkeval.conf

import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.bigsr.fwk.rdf.RDFTriple

/**
  * @author xiangnan ren
  */
class RDFTripleSchema(topic: String) extends SerializationSchema[RDFTriple]
  with DeserializationSchema[RDFTriple] {

  override def serialize(element: RDFTriple): Array[Byte] = {
    FlinkRDFTripleSerializer.serialize(topic, element)
  }

  override def isEndOfStream(nextElement: RDFTriple): Boolean = false

  override def deserialize(message: Array[Byte]): RDFTriple = {
    FlinkRDFTripleDeserializer.deserialize(topic, message)
  }

  override def getProducedType: TypeInformation[RDFTriple] = {
    createTypeInformation[RDFTriple]
  }
}
