package program.graph


import org.bigsr.fwk.program.formula.atom.Atom
import org.bigsr.fwk.program.graph.stratify.{DAG, TarjanProc}
import org.bigsr.fwk.program.graph.{CompleteDepGraph, Edge, Vertex}
import org.scalatest.FunSuite

/**
  * @author xiangnan ren
  */
class DAGTest extends FunSuite {
  val a = Vertex(Atom("a"))
  val b = Vertex(Atom("b"))
  val c = Vertex(Atom("c"))
  val d = Vertex(Atom("d"))
  val e = Vertex(Atom("e"))
  val f = Vertex(Atom("f"))
  val g = Vertex(Atom("g"))
  val h = Vertex(Atom("h"))

  val ab = Edge(a, b, "")
  val af = Edge(a, f, "")
  val fe = Edge(f, e, "")
  val ea = Edge(e, a, "")
  val bc = Edge(b, c, "")
  val cg = Edge(c, g, "")
  val gc = Edge(g, c, "")
  val fg = Edge(f, g, "")
  val bf = Edge(b, f, "")
  val cd = Edge(c, d, "")
  val hg = Edge(h, g, "")

  val graph = new CompleteDepGraph(Set(a, b, c, d, e, f, g, h),
    Set(ab, af, fe, ea, bc, cg, gc, fg, bf, cd, hg))
  assert(TarjanProc.stratify(graph).nestedVerticesSet ===
    Set(Set(h), Set(a, b, e, f), Set(c, g), Set(d)))
  val dag = DAG(graph)

  dag.sccVertices.foreach(x => println(x))

  test("Creation of leaf nodes") {
    dag.createLeafVertex() === Map(6 -> Set(d))
  }

  test("Create edges of scc") {
    assert(dag.sccVertices.map(x => x.nestedEdges).filter(_.nonEmpty) === Set(
      Set(af.reversed, bf.reversed, ab.reversed, fe.reversed, ea.reversed),
      Set(cg.reversed, gc.reversed)))
  }

  test("Upstream/Downstream, linear Test") {
    assert(dag.dct.upstreamIdDct === Map(15 -> Set(7), 9 -> Set(7), 7 -> Set(6)))
    assert(dag.dct.downstreamIdDct === Map(7 -> Set(15, 9), 6 -> Set(7)))
    assert(dag.isLinear === true)
  }
}
