package org.bigsr.fwk.rdf

import org.bigsr.fwk.common.Fact

/**
  * @author xiangnan ren
  */

/**
  * A naive representation of RDF Triple, without considering the data type.
  * Note that there are two ways to wrap RDF triple as a fact, i.e.,
  *    1. predicate(subject, object)
  *    2. triple(subject, predicate, object)
  */
case class RDFTriple(subject: String,
                     predicate: String,
                     `object`: String) extends Serializable {
  override def toString: String = s"$subject $predicate ${`object`}"

  def asFact: Fact = Seq(subject, `object`)

  def asTripleFact: Fact = Seq(subject, predicate, `object`)

}