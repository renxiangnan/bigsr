package spark

import org.bigsr.engine.core.spark.sparkeval.conf.SparkSingleton
import org.bigsr.fwk.program.Program
import org.bigsr.fwk.program.formula.atom.{Atom, Term}
import org.bigsr.fwk.program.rule.Rule
import org.scalatest.FunSuite

/**
  * @author xiangnan ren
  */
trait SharedSparkContext extends FunSuite {
  @transient implicit var spark = SparkSingleton.
    init(true: Boolean, 1, 1, 1, 10).spark

  protected def convertToString(input: Seq[Long]): Seq[String] = {
    input.map(e => e.toString)
  }

  protected def getProgram(choice: String): Program = {
    choice match {
      case "p_1" =>
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
        Program(Set(
          rule1, rule2, rule3, rule4, rule5,
          rule6, rule7, rule8, rule9, rule10))

      case "p_2" =>
        val atom_R_XY = Atom("R", Term("X"), Term("Y"))
        val atom_R_XZ = Atom("R", Term("X"), Term("Z"))
        val atom_T_XY = Atom("T", Term("X"), Term("Y"))
        val atom_T_ZY = Atom("T", Term("Z"), Term("Y"))

        val rule1 = Rule(atom_T_XY, Set(atom_R_XY))
        val rule2 = Rule(atom_T_XY, Set(atom_R_XZ, atom_T_ZY))
        Program(Set(rule1, rule2))

      case "p_3" =>
        val atom_R_XZ = Atom("R", Term("X"), Term("Z"))
        val atom_R_ZY = Atom("R", Term("Z"), Term("Y"))
        val atom_S_XY = Atom("S", Term("X"), Term("Y"))
        val atom_T_XY = Atom("T", Term("X"), Term("Y"))
        val atom_T_XZ = Atom("T", Term("X"), Term("Z"))
        val atom_T1_ZY = Atom("T1", Term("Z"), Term("Y"))

        val rule1 = Rule(atom_T_XY, Set(atom_R_XZ, atom_R_ZY))
        val rule2 = Rule(atom_T1_ZY, Set(atom_R_ZY))
        val rule3 = Rule(atom_S_XY, Set(atom_T_XZ, atom_T1_ZY))
        Program(Set(rule1, rule2, rule3))

      case "p_4" =>
        val atom_R_XY = Atom("R", Term("X"), Term("Y"))
        val atom_T_XY = Atom("T", Term("X"), Term("Y"))
        val rule = Rule(atom_T_XY, Set(atom_R_XY))
        Program(Set(rule))
    }
  }
}
