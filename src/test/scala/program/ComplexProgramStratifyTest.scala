package program

import org.bigsr.fwk.program.Program
import org.bigsr.fwk.program.formula.atom.{Atom, Term}
import org.bigsr.fwk.program.rule.Rule
import org.scalatest.FunSuite

/**
  * @author xiangnan ren
  */
class ComplexProgramStratifyTest extends FunSuite {
  val atom_E_XY = Atom("E", Term("X"), Term("Y"))
  val atom_Odd_X = Atom("Odd", Term("X"))
  val atom_T11_XY = Atom("T11", Term("X"), Term("Y"))
  val atom_T11_XZ = Atom("T11", Term("X"), Term("Z"))
  val atom_E_ZY = Atom("E", Term("Z"), Term("Y"))
  val atom_Odd_Z = Atom("Odd", Term("Z"))
  val atom_T21_XY = Atom("T21", Term("X"), Term("Y"))
  val atom_T22_XZ = Atom("T22", Term("X"), Term("Z"))
  val atom_T22_XY = Atom("T22", Term("X"), Term("Y"))
  val atom_Even_X = Atom("Even", Term("X"))
  val atom_Even_Z = Atom("Even", Term("Z"))
  val atom_T12_XY = Atom("T12", Term("X"), Term("Y"))
  val atom_T_XY = Atom("T", Term("X"), Term("Y"))

  /** group 1:
    * T11(x,y) :- E(x,y), Odd(x)   {Init}
    *
    * T11(x,y) :- T11(x,z), E(z,y), Odd(z)
    * T11(x,y) :- T21(x,y)
    *
    * group 2:
    * T12(x,y) :- T11(x,z), E(z,y), Even(z)
    *
    * group 3:
    * T21(x,y) :- T22(x,z), E(z,y), Odd(z)
    *
    * group 4:
    * T22(x,y) :- E(x,y), Even(x)   {Init}
    *
    * T22(x,y) :- T22(x,z), E(z,y), Even(z)
    * T22(x,y) :- T12(x,y)
    */
  val rule1 = Rule(atom_T11_XY, Set(atom_E_XY, atom_Odd_X))
  val rule2 = Rule(atom_T11_XY, Set(atom_T11_XZ, atom_E_ZY, atom_Odd_Z))
  val rule3 = Rule(atom_T21_XY, Set(atom_T22_XZ, atom_E_ZY, atom_Odd_Z))
  val rule4 = Rule(atom_T11_XY, Set(atom_T21_XY))
  val rule5 = Rule(atom_T22_XY, Set(atom_E_XY, atom_Even_X))
  val rule6 = Rule(atom_T22_XY, Set(atom_T22_XZ, atom_E_ZY, atom_Even_Z))
  val rule7 = Rule(atom_T12_XY, Set(atom_T11_XZ, atom_E_ZY, atom_Even_Z))
  val rule8 = Rule(atom_T22_XY, Set(atom_T12_XY))
  val rule9 = Rule(atom_T_XY, Set(atom_T11_XY))
  val rule10 = Rule(atom_T_XY, Set(atom_T22_XY))

  val program = Program(Set(
    rule1, rule2, rule3, rule4, rule5,
    rule6, rule7, rule8, rule9, rule10))

  test("Stratify Test") {
    val stratum1 = Set(rule1, rule2, rule3, rule4,
      rule5, rule6, rule7, rule8)
    val stratum2 = Set(rule9, rule10)

    assert(program.gatherRulesByStratum.
      map(s => s.rules) === Set(stratum1, stratum2))
    assert(program.isStratified === true)
    assert(program.isRecursive === true)
  }

  test("EDB predicate creation test") {
    assert(program.getEDBPredicates === Set("E", "Odd", "Even"))
  }
}
