package org.bigsr.engine.core.spark.stream

import java.io.ByteArrayOutputStream

import com.esotericsoftware.kryo.io.Input
import kafka.serializer.{Decoder, Encoder}
import kafka.utils.VerifiableProperties
import org.bigsr.engine.core.spark.sparkeval.conf.{SparkDeserializer, SparkSerializer}
import org.bigsr.fwk.rdf.RDFTriple

/**
  * @author xiangnan ren
  */

private[spark]
class StreamSerializer[T](props: VerifiableProperties = null)
  extends Encoder[T] {
  override def toBytes(t: T): Array[Byte] = {
    val byteArrayOutput = new ByteArrayOutputStream()
    val output = SparkSerializer.kryoSerializer.newKryoOutput()

    output.setOutputStream(byteArrayOutput)
    SparkSerializer.kryos.get().writeClassAndObject(output, t)

    output.close()
    byteArrayOutput.toByteArray
  }
}

private[spark]
class StreamDeserializer[T](props: VerifiableProperties = null)
  extends Decoder[T] {
  override def fromBytes(messageBytes: Array[Byte]): T = {
    val input = new Input()
    input.setBuffer(messageBytes)

    //    val message = SparkDeserializer.kryos.get().readClassAndObject(input)
    val message = SparkDeserializer.kryos.
      get().readObject(input, classOf[RDFTriple])

    message.asInstanceOf[T]
  }
}