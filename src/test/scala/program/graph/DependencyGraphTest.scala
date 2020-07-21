package program.graph

import org.bigsr.fwk.program.formula.atom.{Atom, Term}
import org.bigsr.fwk.program.graph.{CompleteDepGraph, Edge, StdDepGraph, Vertex}
import org.bigsr.fwk.program.rule.Rule
import org.scalatest.FunSuite


/**
  * @author xiangnan ren
  */
class DependencyGraphTest extends FunSuite {

  test("Standard Dependency Graph1") {
    val bodyAtom1 = Atom(true, "R", Term("X"), Term("Y"))
    val bodyAtom2 = Atom(true, "R", Term("X"), Term("Z"))
    val bodyAtom3 = Atom(true, "T", Term("Z"), Term("Y"))
    val headAtom1 = Atom(true, "T", Term("X"), Term("Y"))
    val headAtom2 = Atom(true, "T", Term("X"), Term("Y"))

    val rule1 = Rule(headAtom1, Set(bodyAtom1))
    val rule2 = Rule(headAtom2, Set(bodyAtom2, bodyAtom3))

    val vertex1 = new Vertex(rule1.head)
    val vertex2 = new Vertex(rule2.head)
    assert(vertex1 === vertex2)

    val edge1 = Edge(
      Vertex(Atom(true, "T", Term("X1"), Term("Y1"))),
      Vertex(Atom(true, "T", Term("X2"), Term("Y2"))), "")
    val edge2 = Edge(
      Vertex(Atom(true, "T", Term("X1"), Term("Y1"))),
      Vertex(Atom(true, "T", Term("X2"), Term("Y2"))), "")
    assert(edge1 === edge2)

    val stdGraph = StdDepGraph(Set(rule1, rule2))
    println(s"Input program: ${Set(rule1, rule2)}, \n" +
      s"Dependency graph: ${stdGraph.edges} \n")
  }

  test("Standard/Complete Dependency Graph2") {
    val atom1 = Atom("FirstSequelOf", Term("X"), Term("Y"))
    val atom2 = Atom("FollowOn", Term("X"), Term("Y"))
    val atom3 = Atom("FirstSequelOf", Term("X"), Term("Z"))
    val atom4 = Atom("FollowOn", Term("Z"), Term("Y"))
    val atom5 = Atom("SequelOf", Term("X"), Term("Y"))

    val rule1 = Rule(atom1, Set(atom5))
    val rule2 = Rule(atom2, Set(atom1))
    val rule3 = Rule(atom2, Set(atom3, atom4))

    val stdDepGraph = StdDepGraph(Set(rule1, rule2, rule3))
    println(s"Input program: ${Set(rule1, rule2, rule3)}, \n" +
      s"Dependency graph: ${stdDepGraph.edges} \n")

    val completeGraph = CompleteDepGraph(Set(rule1, rule2, rule3))
    println(s"Vertices: ${completeGraph.vertices}, \n" +
      s"Edges: ${completeGraph.edges}\n")

    assert(completeGraph.simplify().vertices ===
      Set(Vertex(Atom("SequelOf")),
        Vertex(Atom("FirstSequelOf")),
        Vertex(Atom("FollowOn"))))

    assert(completeGraph.simplify().edges ===
      Set(Edge(Vertex(Atom("FirstSequelOf")), Vertex(Atom("SequelOf")), ""),
        Edge(Vertex(Atom("FollowOn")), Vertex(Atom("FirstSequelOf")), ""),
        Edge(Vertex(Atom("FollowOn")), Vertex(Atom("FollowOn")), "")))


  }
}
