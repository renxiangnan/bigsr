package org.bigsr.fwk.program.compiler.algebra

import org.bigsr.fwk.program.NonUniformException
import org.bigsr.fwk.program.compiler._
import org.bigsr.fwk.program.formula.atom.Atom
import org.bigsr.fwk.program.rule.Rule

import scala.collection.mutable

/**
  * @author xiangnan ren
  */

/**
  * Build the SPJU (left-deep) algebra tree for each intensional predicate.
  * The input rules should have the same head atom.
  *
  * We suppose that the body of each rule does not contain many atoms
  * (<= 3 in general), i.e. the optimization of algebra tree becomes insignificant.
  *
  * @param relationName  : The corresponding intensional name
  * @param idbPredicates : All IDB predicates of the corresponding stratum
  * @param rules         : The rules concerned the given intensional predicate
  * @param reducedRules  : The rules only contain EDB predicates for a given IDB predicate
  */
class AlgebraBuilder(relationName: String,
                     idbPredicates: Set[String],
                     rules: Set[Rule],
                     reducedRules: Set[Rule]) {
  if (!rules.forall(_.head.getAtom.getPredicate == relationName))
    throw NonUniformException("" +
      "The input rules should contain the same head atom")

  def createAlgebraTree: Op = combineSubTree(rules)

  private def combineSubTree(rules: Set[Rule]): Op = {
    val stack = mutable.Stack[Op]()
    val rulesSeq = rules.toSeq

    rulesSeq.indices.foreach { i =>
      if (i >= 1) {
        val rightOp = constructSubTree(rulesSeq(i))
        val leftOp = stack.pop
        stack.push(Union(leftOp, rightOp))
      } else stack.push(constructSubTree(rulesSeq(i)))
    }

    stack.pop()
  }

  def createAlgebraTreeSet: Set[Op] = combineSubTreeSet(rules)

  private def combineSubTreeSet(rules: Set[Rule]): Set[Op] = {
    rules.map(constructSubTree)
  }

  /**
    * Construct the algebra tree for each rule.
    * The schema of projection relation (the root of the tree)
    * refers to the schema of the rule's head atom.
    */
  def constructSubTree(rule: Rule): Op = {
    val stack = mutable.Stack[Op]()
    val schema = rule.head.getAtom.schema

    rule.subgoalSeq.indices.foreach { i =>
      rule.subgoalSeq(i) match {
        case a: Atom =>
          val name =
            if (idbPredicates.contains(a.getPredicate)) {
              deltaKey(a.getPredicate)
            } else a.getPredicate

          if (i >= 1) stack.push(
            Join(stack.pop(), Selection(name, a), rule.param))
          else stack.push(Selection(name, a))

        case _ => throw new UnsupportedOperationException(
          "Negation and built-in atom are not supported yet")
      }
    }

    if (rule.subgoals.size > 1) stack.push(Projection(relationName, schema, stack.pop))
    else stack.push(Projection(relationName, schema, stack.pop, rule.param))

    stack.pop
  }

  def createInitAlgebraTree: Op = combineSubTree(reducedRules)
}

object AlgebraBuilder {
  def apply(relationName: String,
            idbPredicates: Set[String],
            rules: Set[Rule],
            reducedRules: Set[Rule]): AlgebraBuilder =
    new AlgebraBuilder(relationName, idbPredicates, rules, reducedRules)

}
