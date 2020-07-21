package org.bigsr.fwk.stream

import java.util.Properties

import scala.collection.mutable

/**
  * @author xiangnan ren
  */
case class KafkaConsumerParam(brokerAddr: String,
                              zkConnection: String,
                              groupId: String,
                              params: Option[Map[String, String]],
                              topics: String*) {
  val topicsSet: Set[String] = setTopics().get
  val kafkaParams: Map[String, String] = setDirectConf()

  def asFlinkProperties(): Properties = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", brokerAddr)
    properties.setProperty("zookeeper.connect", zkConnection)
    properties.setProperty("group.id", groupId)
    properties
  }

  private def setTopics(): Option[Set[String]] = {
    try {
      val topicsSet = mutable.Set[String]()
      topics.foreach(topic => topicsSet.add(topic))
      Some(topicsSet.toSet)
    } catch {
      case e: Exception => e.printStackTrace(); None;
    }
  }

  private def setDirectConf(): Map[String, String] = {
    Map("metadata.broker.list" -> brokerAddr)
  }

}

