package org.bigsr.fwk.program.formula.atom

/**
  * @author xiangnan ren
  */

class Atom(val isPositive: Boolean,
           val predicate: String,
           val terms: Term*) extends ExtendedAtom {
  val schema = terms

  def this(predicate: String,
           terms: Term*) = this(true, predicate, terms: _*)

  override def getAtom: Atom = this

  override def variables: Set[String] = {
    for (t <- terms if !t.isConstant) yield t.toString
  }.toSet

  override def equals(obj: scala.Any): Boolean =
    if (!obj.isInstanceOf[Atom]) false
    else {
      val atom = obj.asInstanceOf[Atom]

      this.isPositive == atom.isPositive &&
        this.predicate == atom.getPredicate &&
        this.terms.corresponds(atom.terms) {
          _ == _
        }
    }

  override def getPredicate: String = predicate

  override def hashCode(): Int = this.getPredicate.hashCode

  override def toString: String = s"$predicate(${terms.mkString(", ")})"

}

object Atom {
  def apply(isPositive: Boolean,
            predicate: String,
            terms: Term*): Atom =
    new Atom(isPositive: Boolean, predicate, terms: _*)

  def apply(predicate: String,
            terms: Term*) =
    new Atom(predicate, terms: _*)
}