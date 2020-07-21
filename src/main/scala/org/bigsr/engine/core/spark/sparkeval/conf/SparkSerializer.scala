package org.bigsr.engine.core.spark.sparkeval.conf

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoSerializer

/**
  * @author xiangnan ren
  */
private[spark]
object SparkSerializer {
  val kryoSerializer = new KryoSerializer(SparkSingleton.conf)
  val kryo: Kryo = kryoSerializer.newKryo()

  val kryos: ThreadLocal[Kryo] = new ThreadLocal[Kryo]() {
    protected override def initialValue(): Kryo = {
      kryoSerializer.newKryo()
    }
  }
}

private[spark]
object SparkDeserializer {
  val kryoDeserializer = new KryoSerializer(SparkSingleton.conf)
  val kryo: Kryo = new KryoSerializer(SparkSingleton.conf).newKryo()

  val kryos: ThreadLocal[Kryo] = new ThreadLocal[Kryo]() {
    protected override def initialValue(): Kryo = {
      kryoDeserializer.newKryo()
    }
  }
}
