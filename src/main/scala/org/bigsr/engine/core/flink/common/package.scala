package org.bigsr.engine.core.flink

import org.apache.flink.streaming.api.scala.DataStream
import org.bigsr.fwk.common.Fact

import scala.collection.mutable

/**
  * @author xiangnan ren
  */
package object common {
  private[flink] type FlinkEDBMap = Map[String, DataStream[Fact]]
  private[flink] type FlinkStreamMap = mutable.Map[String, DataStream[Fact]]

}
