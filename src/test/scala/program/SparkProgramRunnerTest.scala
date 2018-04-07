package program

import org.bigsr.engine.core.spark.sparkeval.evaluator.SparkProgramRunner
import org.bigsr.fwk.program.compiler.algebra.AlgebraPrettyPrinter

/**
  * @author xiangnan ren
  */
class SparkProgramRunnerTest extends ComplexProgramStratifyTest {
  val wf = SparkProgramRunner(program)
  val allPredicates = program.getEDBPredicates
  val chainedOps = wf.initLogicalPlan

  test("Linear DAG op test") {
    assert(chainedOps(0).param.isRecursive === true)
    assert(chainedOps(1).param.isRecursive === false)

    assert(chainedOps.map(_.param.rules).toSet ===
      Set(
        Set(rule1, rule2, rule3, rule4, rule5, rule6, rule7, rule8),
        Set(rule9, rule10)))
    assert(chainedOps(0).param.groupByPredicate ===
      Map("T11" -> Set(rule1, rule2, rule4),
        "T12" -> Set(rule7),
        "T21" -> Set(rule3),
        "T22" -> Set(rule5, rule6, rule8)))
    assert(chainedOps(1).param.groupByPredicate === Map("T" -> Set(rule9, rule10)))
    assert(chainedOps(0).param.allSubgoals.map(_.toString) ===
      Set("E(X, Y)", "E(Z, Y)", "T12(X, Y)", "Odd(Z)",
        "Odd(X)", "T22(X, Z)", "T11(X, Z)", "Even(X)", "Even(Z)", "T21(X, Y)"),
      chainedOps(1).param.allSubgoals.map(_.toString) ===
        Set("T22(x,z)", "E(z,y)", "Even(z)", "T12(x,y)"))

    assert(chainedOps(0).param.inferredIDBPredicates === Set() &&
      chainedOps(1).param.inferredIDBPredicates === Set("T11", "T22") &&
      chainedOps(0).param.localEDBPredicates === Set("E", "Odd", "Even") &&
      chainedOps(1).param.localEDBPredicates === Set() &&
      chainedOps(0).param.depPredicates === Set("E", "Odd", "Even") &&
      chainedOps(1).param.depPredicates === Set("T11", "T22"))

    chainedOps(1).param.combinedAlgebra.foreach { x =>
      println("key:" + x._1)
      AlgebraPrettyPrinter.print(x._2)
    }

    assert(chainedOps(0).param.combinedAlgebra.size === 4 &&
      chainedOps(1).param.combinedAlgebra.size === 1)

    println(chainedOps(0).param.combinedAlgebra.head._1)
    println("Leaves: " + chainedOps(0).param.combinedAlgebra.head._2.leaves)
  }

}
