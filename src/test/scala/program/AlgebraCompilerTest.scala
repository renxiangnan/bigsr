package program

import org.bigsr.fwk.program.NonUniformException
import org.bigsr.fwk.program.compiler.algebra.AlgebraBuilder

/**
  * @author xiangnan ren
  */
class AlgebraCompilerTest extends ComplexProgramStratifyTest {
  test("Algebra builder exception") {
    intercept[NonUniformException] {
      AlgebraBuilder(rule1.head.getPredicate, Set(""), Set(rule1, rule2, rule3), Set())
    }
  }
  test("Algebra builder test") {
    val rules = Set(rule1, rule2)
    val algebraBuilder =
      AlgebraBuilder("T11", Set("T11", "T12", "T21", "T22"), rules, Set())
    val op = algebraBuilder.constructSubTree(rule2)
    val opCombined = algebraBuilder.createAlgebraTree

    assert(opCombined.leaves === Set("E", "Odd", "DELTA_T11"))
  }
}
