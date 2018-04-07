package org.bigsr.engine.core.flink.flinkeval.conf

import java.util

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ByteBufferInput, ByteBufferOutput, Input, Output}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.bigsr.fwk.rdf.RDFTriple


/**
  * @author xiangnan ren
  */
private[flink]
object FlinkRDFTripleSerializer extends Serializer[RDFTriple] {
  val kryos: ThreadLocal[Kryo] = new ThreadLocal[Kryo]() {
    protected override def initialValue(): Kryo = {
      val kryo = new Kryo()
      kryo.addDefaultSerializer(
        classOf[RDFTriple],
        new KryoInternalSerializer())
      kryo
    }
  }

  override def configure(configs: util.Map[String, _],
                         isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def serialize(topic: String,
                         triple: RDFTriple): Array[Byte] = {
    val output = new ByteBufferOutput(100)
    kryos.get().writeObject(output, triple)
    output.toBytes
  }
}

private[flink]
object FlinkRDFTripleDeserializer extends Deserializer[RDFTriple] {
  val kryos: ThreadLocal[Kryo] = new ThreadLocal[Kryo]() {
    protected override def initialValue(): Kryo = {
      val kryo = new Kryo()
      kryo.addDefaultSerializer(classOf[RDFTriple], new KryoInternalSerializer())
      kryo
    }
  }

  override def configure(configs: util.Map[String, _],
                         isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def deserialize(topic: String,
                           bytes: Array[Byte]): RDFTriple = {
    try {
      kryos.get().readObject(new ByteBufferInput(bytes), classOf[RDFTriple])
    } catch {
      case e: Throwable => throw new IllegalArgumentException("Error reading bytes", e)
    }
  }
}


private[flink]
class KryoInternalSerializer extends
  com.esotericsoftware.kryo.Serializer[RDFTriple] {
  override def read(kryo: Kryo,
                    input: Input,
                    `type`: Class[RDFTriple]): RDFTriple = {
    // Retrieve the element in reverse order
    val o = kryo.readObject(input, classOf[String])
    val p = kryo.readObject(input, classOf[String])
    val s = kryo.readObject(input, classOf[String])

    RDFTriple(s, p, o)
  }

  override def write(kryo: Kryo,
                     output: Output,
                     triple: RDFTriple): Unit = {
    output.writeString(triple.subject)
    output.writeString(triple.predicate)
    output.writeString(triple.`object`)

  }
}