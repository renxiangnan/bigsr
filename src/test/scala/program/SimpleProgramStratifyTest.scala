package program

import org.bigsr.fwk.program.Program
import org.bigsr.fwk.program.formula.atom.{Atom, Term}
import org.bigsr.fwk.program.graph.{Edge, Vertex}
import org.bigsr.fwk.program.rule.Rule
import org.scalatest.FunSuite

/**
  * @author xiangnan ren
  */
class SimpleProgramStratifyTest extends FunSuite {
  val atom_R_XY = Atom("R", Term("X"), Term("Y"))
  val atom_R_XZ = Atom("R", Term("X"), Term("Z"))
  val atom_T_XY = Atom("T", Term("X"), Term("Y"))
  val atom_T_ZY = Atom("T", Term("Z"), Term("Y"))
  val rule1 = Rule(atom_T_XY, Set(atom_R_XY))

  test("Non Stratified Program") {
    val atom_T_ZY = Atom(false, "T", Term("Z"), Term("Y"))
    val rule2 = Rule(atom_T_XY, Set(atom_R_XZ, atom_T_ZY))
    val program = Program(Set(rule1, rule2))

    assert(program.getDepGraph.vertices ===
      Set(Vertex(atom_T_XY), Vertex(atom_R_XZ)))
    assert(program.getDepGraph.edges === Set(
      Edge(Vertex(atom_T_XY), Vertex(atom_T_ZY)),
      Edge(Vertex(atom_T_XY), Vertex(atom_R_XZ))))
    assert(program.isStratified === false)
  }

  test("Stratified Program") {
    val atom_T_ZY = Atom(true, "T", Term("Z"), Term("Y"))
    val rule2 = Rule(atom_T_XY, Set(atom_R_XZ, atom_T_ZY))
    val program = Program(Set(rule1, rule2))

    assert(program.getDepGraph.vertices ===
      Set(Vertex(atom_T_XY), Vertex(atom_R_XZ)))
    assert(program.getDepGraph.edges === Set(
      Edge(Vertex(atom_T_XY), Vertex(atom_T_ZY)),
      Edge(Vertex(atom_T_XY), Vertex(atom_R_XZ))))
    assert(program.isStratified === true)
  }

}
