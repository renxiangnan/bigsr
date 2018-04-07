package org.bigsr.fwk.program.rule

import org.bigsr.fwk.program.EmptyJoinKeyException
import org.bigsr.fwk.program.formula.TimeWindowParam
import org.bigsr.fwk.program.formula.atom.ExtendedAtom

import scala.collection.mutable

/**
  * @author xiangnan ren
  */

/**
  * The class implements "standard" rule.
  *
  */
case class Rule(head: ExtendedAtom,
                subgoals: Set[ExtendedAtom],
                param: Option[TimeWindowParam] = None) extends RuleBase {
  val subgoalSeq: Seq[ExtendedAtom] = subgoals.toSeq

  override def toString: String = s"$head :- ${subgoals.mkString(", ")}"

  def allAtoms: Set[ExtendedAtom] = subgoals + head

  def allPredicates: Set[String] = subgoals.map(_.getPredicate) + head.getPredicate

  def inHead(atom: ExtendedAtom): Boolean = this.head.equals(atom)

  def inBody(atom: ExtendedAtom): Boolean = this.subgoals.contains(atom)


  def createSubGoalSeq: Seq[ExtendedAtom] = {
    val keyCandidates = mutable.Set[String]() ++ subgoalSeq.head.variables
    val _subgoalSeq = subgoalSeq.map(identity).toBuffer

    for (_ <- 0 until subgoals.size)
      yield _subgoalSeq.find { a =>
        keyCandidates.intersect(a.variables).nonEmpty
      }
      match {
        case Some(_atom) =>
          keyCandidates ++= _atom.variables
          _subgoalSeq -= _atom
          _atom
        case None =>
          throw EmptyJoinKeyException("The input rule causes cartesian product.")

      }

  }

}
