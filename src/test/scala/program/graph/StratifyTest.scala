package program.graph


import org.bigsr.fwk.program.formula.atom.Atom
import org.bigsr.fwk.program.graph.stratify.TarjanProc
import org.bigsr.fwk.program.graph.{CompleteDepGraph, Edge, Vertex}
import org.scalatest.FunSuite


/**
  * @author xiangnan ren
  */
class StratifyTest extends FunSuite {
  val a = Vertex(Atom("a"))
  val b = Vertex(Atom("b"))
  val c = Vertex(Atom("c"))
  val d = Vertex(Atom("d"))
  val e = Vertex(Atom("e"))
  val f = Vertex(Atom("f"))
  val g = Vertex(Atom("g"))
  val h = Vertex(Atom("h"))

  test("Stratify Test 1, single leaf node") {
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

    val graph = new CompleteDepGraph(
      Set(a, b, c, d, e, f, g, h),
      Set(ab, af, fe, ea, bc, cg, gc, fg, bf, cd, hg))
    assert(TarjanProc.stratify(graph).nestedVerticesSet ===
      Set(Set(h), Set(a, b, e, f), Set(c, g), Set(d)))
  }

  test("Stratify Test 2, single leaf node") {
    val ab = Edge(a, b, "")
    val bf = Edge(b, f, "")
    val be = Edge(b, e, "")
    val ef = Edge(e, f, "")
    val ea = Edge(e, a, "")
    val bc = Edge(b, c, "")
    val fg = Edge(f, g, "")
    val gf = Edge(g, f, "")
    val cg = Edge(c, g, "")
    val cd = Edge(c, d, "")
    val dc = Edge(d, c, "")
    val dh = Edge(d, h, "")
    val gh = Edge(g, h, "")
    val hh = Edge(h, h, "")

    val graph = new CompleteDepGraph(Set(a, b, c, d, e, f, g, h),
      Set(ab, bf, be, ef, ea, bc, fg, gf, cg, cd, dc, dh, gh, hh))
    assert(TarjanProc.stratify(graph).nestedVerticesSet ===
      Set(Set(a, b, e), Set(c, d), Set(f, g), Set(h)))
  }

  test("Stratify Test 3, two leaf nodes") {
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
    val gh = Edge(g, h, "")

    val graph = new CompleteDepGraph(
      Set(a, b, c, d, e, f, g, h),
      Set(ab, af, fe, ea, bc, cg, gc, fg, bf, cd, gh))
    assert(TarjanProc.stratify(graph).nestedVerticesSet ===
      Set(Set(h), Set(a, b, e, f), Set(c, g), Set(d)))
  }

  test("DAG Test") {
    val ab = Edge(a, b, "")
    val bc = Edge(b, c, "")
    val cd = Edge(c, d, "")
    val af = Edge(a, f, "")
    val bf = Edge(b, f, "")
    val ef = Edge(e, f, "")
    val fg = Edge(f, g, "")
    val hg = Edge(h, g, "")
    val gc = Edge(g, c, "")

    val graph = new CompleteDepGraph(Set(a, b, c, d, e, f, g, h),
      Set(ab, bc, cd, af, bf, ef, fg, hg, gc))
    assert(TarjanProc.stratify(graph.simplify()).nestedVerticesSet.size === 8)
  }
}
