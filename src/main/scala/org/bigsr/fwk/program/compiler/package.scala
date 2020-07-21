package org.bigsr.fwk.program

import scala.collection.{Map, mutable}

/**
  * @author xiangnan ren
  */
package object compiler {
  def deltaKey(key: String): String = "DELTA_" + key

  def toMutable[K, V](m: Map[K, V]): mutable.Map[K, V] =
    scala.collection.mutable.Map() ++= m
}
