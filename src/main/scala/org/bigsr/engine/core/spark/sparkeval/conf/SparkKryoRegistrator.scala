package org.bigsr.engine.core.spark.sparkeval.conf

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.bigsr.fwk.program.compiler.algebra.{Op, Op0, Op1, Op2}
import org.bigsr.fwk.rdf.RDFTriple

import scala.collection.mutable

/**
  * @author xiangnan ren
  */
private[spark]
class SparkKryoRegistrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[String])
    kryo.register(classOf[Array[Object]])
    kryo.register(classOf[Array[String]])
    kryo.register(classOf[Array[Seq[Long]]])
    kryo.register(classOf[Seq[_]])
    kryo.register(classOf[mutable.WrappedArray.ofChar])
    kryo.register(classOf[RDFTriple])
    kryo.register(classOf[Array[RDFTriple]])
    kryo.register(Class.forName("[[B"))
    kryo.register(classOf[Op])
    kryo.register(classOf[Op0])
    kryo.register(classOf[Op1])
    kryo.register(classOf[Op2])
    kryo.register(Class.forName("scala.collection.mutable.WrappedArray$ofRef"))
    kryo.register(Class.forName("scala.collection.mutable.WrappedArray$ofInt"))
    kryo.register(Class.forName("scala.reflect.ClassTag$$anon$1", false, getClass.getClassLoader))
    kryo.register(Class.forName("java.lang.Class"))

  }
}
