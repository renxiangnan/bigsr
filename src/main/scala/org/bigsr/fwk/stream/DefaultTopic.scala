package org.bigsr.fwk.stream

/**
  * @author xiangnan ren
  */
trait DefaultTopic {
  val topic: String
}

case object MsgRDFTriple extends DefaultTopic {
  val topic = "MsgRDFTriple"
}
