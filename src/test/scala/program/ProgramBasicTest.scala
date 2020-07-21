package program

import org.bigsr.fwk.program.EmptyJoinKeyException
import org.bigsr.fwk.program.formula.atom.{Atom, Constant, Term, Variable}
import org.bigsr.fwk.program.graph.StdDepGraph
import org.bigsr.fwk.program.rule.Rule
import org.scalatest.FunSuite

/**
  * @author xiangnan ren
  */
class ProgramBasicTest extends FunSuite {
  val bodyAtom1 = Atom(true, "predicate_1", Term("X"), Term("1"))
  val bodyAtom2 = Atom(true, "predicate_2", Term("Y"), Term("3"))
  val headAtom = Atom(true, "predicate_3", Term("X"), Term("Y"))
  val rule = Rule(headAtom, Set(bodyAtom1, bodyAtom2))

  test("Rule Equality") {
    val _bodyAtom1 = Atom(true, "predicate_1", Term("X"), Term("1"))
    val _bodyAtom2 = Atom(true, "predicate_2", Term("Y"), Term("3"))
    val _headAtom = Atom(true, "predicate_3", Term("X"), Term("Y"))
    val _rule = Rule(_headAtom, Set(_bodyAtom1, _bodyAtom2))

    assert(bodyAtom1.toString === "predicate_1(X, 1)")
    assert(bodyAtom1.terms(0).getClass === classOf[Variable])
    assert(bodyAtom1.terms(1).getClass === classOf[Constant])
    assert(rule.toString === "predicate_3(X, Y) :- predicate_1(X, 1), predicate_2(Y, 3)")
    assert(rule.equals(_rule) && (rule == _rule) === true)
  }

  test("Extensional/Intesional atom creation") {
    val atoms = StdDepGraph.createAtomSet(Set(rule))
    assert(atoms._1 === Set(headAtom))
    assert(atoms._2 == Set(bodyAtom1, bodyAtom2))
  }

  test("createSubGoalSeq") {
    val atom1 = Atom(true, "predicate_1", Term("X"))
    val atom2 = Atom(true, "predicate_2", Term("Y"), Term("Z"))
    val atom3 = Atom(true, "predicate_3", Term("Z"))
    val atom4 = Atom(true, "predicate_4", Term("X"), Term("Y"))
    val headAtom = Atom(true, "predicate_5", Term("X"), Term("Y"))
    val rule1 = Rule(headAtom, Set(atom1, atom2, atom3, atom4))
    val rule2 = Rule(headAtom, Set(atom1, atom2))
    val rule3 = Rule(headAtom, Set(atom1))

    assert(rule1.createSubGoalSeq === Seq(atom1, atom4, atom2, atom3))
    assert(rule3.createSubGoalSeq === Seq(atom1))
    intercept[EmptyJoinKeyException] {
      rule2.createSubGoalSeq
    }
  }

}
