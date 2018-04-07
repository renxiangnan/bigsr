package org.bigsr.program_examples

import org.bigsr.engine.core.flink.flinkeval.conf.FlinkTimeWindowParam
import org.bigsr.fwk.program.Program
import org.bigsr.fwk.program.formula.atom.{Atom, Term}
import org.bigsr.fwk.program.rule.Rule

/**
  * @author xiangnan ren
  */
object ExampleProgramBuilder {
  private val f_win = (range: Long, slide: Long) => Option(FlinkTimeWindowParam(range, slide))
  //------------------------------ Waves Prefix ------------------------------
  private val rdfSyntaxPref = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  private val ssnPref = "http://purl.oclc.org/NET/ssnx/ssn/"
  private val qudtPref = "http://data.nasa.gov/qudt/owl/qudt/"
  //------------------------------ SRBench Prefix ------------------------------
  private val obsPref = "http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#"
  private val whtPref = "http://knoesis.wright.edu/ssw/ont/weather.owl#"
  //------------------------------ CityBench Prefix ------------------------------
  private val ssnPref1 = "http://purl.oclc.org/NET/ssnx/ssn#"
  private val saoPref = "http://purl.oclc.org/NET/sao/"
  private val servicePref = "http://www.insight-centre.org/dataset/SampleEventService#"
  //------------------------------ Lubm Prefix ------------------------------
  private val lubmPref = "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#"
  //------------------------------ IRI Utils ------------------------------
  private val resIRI = "<http://big-sr/result>"
  private val addTempPref = (s: String) => s"<http://big-sr/temp#$s>"
  private val addPref = (prefix: String, suffix: String) => s"<$prefix$suffix>"


  def apply(p: String,
            range: Long = 1L,
            slide: Long = 1L): Program = p match {
    //------------------------------ Non Recursive ------------------------------
    //------------------------------ Waves Program ------------------------------
    case "NR_Waves_1" =>
      val atom_tp1 = Atom("<http://purl.oclc.org/NET/ssnx/ssn/hasValue>", Term("S"), Term("O"))
      val atom_res = Atom(resIRI, Term("S"))

      val rule1 = Rule(atom_res, Set(atom_tp1), f_win(range, slide))
      Program(Set(rule1))

    case "NR_Waves_2" =>
      val atom_tp1 = Atom(addPref(rdfSyntaxPref, "type"), Term("S"), Term("O1"))
      val atom_tp2 = Atom(addPref(ssnPref, "startTime"), Term("S"), Term("O2"))
      val atom_tp3 = Atom(addPref(qudtPref, "unit"), Term("S"), Term("O3"))
      val atom_tp12 = Atom(addTempPref("tp12"), Term("S"), Term("O2"))
      val atom_result = Atom(resIRI, Term("S"), Term("O3"))

      val rule1 = Rule(atom_tp12, Set(atom_tp1, atom_tp2), f_win(range, slide))
      val rule2 = Rule(atom_result, Set(atom_tp12, atom_tp3), f_win(range, slide))
      Program(Set(rule1, rule2))

    case "NR_Waves_3" =>
      val atom_tp1 = Atom(addPref(rdfSyntaxPref, "type"), Term("S"), Term("O1"))
      val atom_tp2 = Atom(addPref(ssnPref, "startTime"), Term("S"), Term("O2"))
      val atom_tp3 = Atom(addPref(qudtPref, "unit"), Term("S"), Term("O3"))
      val atom_tp4 = Atom(addPref(qudtPref, "numericValue"), Term("S"), Term("O4"))
      val atom_tp5 = Atom(addPref(ssnPref, "isProducedBy"), Term("O"), Term("O5"))
      val atom_tp6 = Atom(addPref(rdfSyntaxPref, "type"), Term("O"), Term("O6"))
      val atom_tp7 = Atom(addPref(ssnPref, "hasValue"), Term("O"), Term("S"))
      val atom_star1 = Atom(addTempPref("star1"), Term("S"), Term("O1"), Term("O2"), Term("O3"), Term("O4"))
      val atom_star2 = Atom(addTempPref("star2"), Term("O"), Term("S"), Term("O5"), Term("O6"))
      val atom_result = Atom(resIRI,
        Term("O"), Term("O1"), Term("O2"), Term("O3"), Term("O4"), Term("O5"), Term("O6"))

      val rule1 = Rule(atom_star1, Set(atom_tp1, atom_tp2, atom_tp3, atom_tp4), f_win(range, slide))
      val rule2 = Rule(atom_star2, Set(atom_tp5, atom_tp6, atom_tp7), f_win(range, slide))
      val rule3 = Rule(atom_result, Set(atom_star1, atom_star2), f_win(range, slide))
      Program(Set(rule1, rule2, rule3))

    //------------------------------ Non Recursive ------------------------------
    //----------------------------- SRBench Program -----------------------------
    case "NR_SRBench_1" =>
      val atom_tp1 = Atom(addPref(obsPref, "procedure"), Term("Obs"), Term("Sen"))
      val atom_tp2 = Atom(addPref(rdfSyntaxPref, "type"), Term("Obs"), Term(addPref(whtPref, "RainfallObservation")))
      val atom_result = Atom(resIRI, Term("Obs"), Term("Sen"))
      val rule1 = Rule(atom_result, Set(atom_tp1, atom_tp2), f_win(range, slide))
      Program(Set(rule1))

    case "NR_SRBench_2" =>
      val atom_tp1 = Atom(addPref(obsPref, "procedure"), Term("Obs"), Term("Sen"))
      val atom_tp2 = Atom(addPref(rdfSyntaxPref, "type"), Term("Obs"), Term(addPref(whtPref, "RainfallObservation")))
      val atom_tp3 = Atom(addPref(obsPref, "result"), Term("Obs"), Term("Res"))
      val atom_star1 = Atom(addTempPref("star1"), Term("Obs"), Term("Sen"))
      val atom_result = Atom(resIRI, Term("Obs"), Term("Res"))

      val rule1 = Rule(atom_star1, Set(atom_tp1, atom_tp2), f_win(range, slide))
      val rule2 = Rule(atom_result, Set(atom_star1, atom_tp3), f_win(range, slide))
      Program(Set(rule1, rule2))

    case "NR_SRBench_3" =>
      val atom_tp1 = Atom(addPref(obsPref, "procedure"), Term("Obs"), Term("Sen"))
      val atom_tp2 = Atom(addPref(rdfSyntaxPref, "type"), Term("Obs"), Term(addPref(whtPref, "RainfallObservation")))
      val atom_tp3 = Atom(addPref(obsPref, "result"), Term("Obs"), Term("Res"))
      val atom_tp4 = Atom(addPref(obsPref, "floatValue"), Term("Res"), Term("Value"))
      val atom_tp5 = Atom(addPref(obsPref, "uom"), Term("Res"), Term("Uom"))
      val atom_star1 = Atom(addTempPref("star1"), Term("Obs"), Term("Res"))
      val atom_star2 = Atom(addTempPref("star2"), Term("Res"), Term("Uom"))
      val atom_result = Atom(resIRI, Term("Obs"), Term("Uom"))

      val rule1 = Rule(atom_star1, Set(atom_tp1, atom_tp2, atom_tp3), f_win(range, slide))
      val rule2 = Rule(atom_star2, Set(atom_tp4, atom_tp5), f_win(range, slide))
      val rule3 = Rule(atom_result, Set(atom_star1, atom_star2), f_win(range, slide))
      Program(Set(rule1, rule2, rule3))


    //------------------------------ Non Recursive ------------------------------
    //---------------------------- CityBench Program ----------------------------
    case "NR_CityBench_1" =>
      val atom_tp1 = Atom(addPref(ssnPref1, "observedBy"), Term("ObId"), Term(addPref(servicePref, "AarhusTrafficData182955")))
      val atom_tp2 = Atom(addPref(saoPref, "hasAvgSpeed"), Term("ObId"), Term("AvgSpeed"))
      val atom_tp3 = Atom(addPref(saoPref, "hasAvgMeasuredTime"), Term("ObId"), Term("AvgMeasuredTime"))
      val atom_tp4 = Atom(addPref(saoPref, "hasVehicleCount"), Term("ObId"), Term("VehicleCount"))
      val atom_res = Atom(resIRI, Term("ObId"), Term("AvgSpeed"))

      val rule = Rule(atom_res, Set(atom_tp1, atom_tp2, atom_tp3, atom_tp4), f_win(range, slide))
      Program(Set(rule))

    case "NR_CityBench_2" =>
      val atom_tp1 = Atom(addPref(ssnPref1, "observedBy"), Term("ObId"), Term(addPref(servicePref, "AarhusTrafficData182955")))
      val atom_tp2 = Atom(addPref(saoPref, "status"), Term("ObId"), Term("Status"))
      val atom_tp3 = Atom(addPref(saoPref, "hasAvgMeasuredTime"), Term("ObId"), Term("AvgMeasuredTime"))
      val atom_tp4 = Atom(addPref(saoPref, "hasAvgSpeed"), Term("ObId"), Term("AvgSpeed"))
      val atom_tp5 = Atom(addPref(saoPref, "hasExtID"), Term("ObId"), Term("ExtID"))
      val atom_tp6 = Atom(addPref(saoPref, "hasMedianMeasuredTime"), Term("ObId"), Term("MedianMeasuredTime"))
      val atom_tp7 = Atom(addPref(ssnPref1, "startTime"), Term("ObId"), Term("StartTime"))
      val atom_tp8 = Atom(addPref(saoPref, "hasVehicleCount"), Term("ObId"), Term("VehicleCount"))
      val atom_star1 = Atom(addTempPref("star1"), Term("ObId"),
        Term("Status"), Term("AvgMeasuredTime"), Term("AvgSpeed"))
      val atom_star2 = Atom(addTempPref("star2"), Term("ObId"), Term("ExtID"),
        Term("MedianMeasuredTime"), Term("StartTime"), Term("VehicleCount"))
      val atom_res = Atom(resIRI, Term("ObId"),
        Term("Status"), Term("AvgMeasuredTime"), Term("AvgSpeed"), Term("ExtID"),
        Term("MedianMeasuredTime"), Term("StartTime"), Term("VehicleCount"))

      val rule1 = Rule(atom_star1, Set(atom_tp1, atom_tp2, atom_tp3, atom_tp4), f_win(range, slide))
      val rule2 = Rule(atom_star2, Set(atom_tp1, atom_tp5, atom_tp6, atom_tp7, atom_tp8), f_win(range, slide))
      val rule3 = Rule(atom_res, Set(atom_star1, atom_star2), f_win(range, slide))
      Program(Set(rule1, rule2, rule3))

    //------------------------------ Non Recursive ------------------------------
    //---------------------------- CityBench Program ----------------------------
    case "R_Lubm_1" =>
      val atom1 = Atom(resIRI, Term("Pub"), Term("Author"))
      val atom2 = Atom(addPref(lubmPref, "publicationAuthor"), Term("Pub"), Term("Author"))
      val atom3 = Atom(addPref(lubmPref, "publicationAuthor"), Term("Pub1"), Term("Author2"))
      val atom4 = Atom(addPref(lubmPref, "publicationAuthor"), Term("Pub2"), Term("Author2"))
      val atom5 = Atom(resIRI, Term("Pub2"), Term("Author1"))
      val atom6 = Atom(resIRI, Term("Pub1"), Term("Author1"))

      val rule1 = Rule(atom1, Set(atom2), f_win(range, slide))
      val rule2 = Rule(atom5, Set(atom6, atom3, atom4), f_win(range, slide))
      Program(Set(rule1, rule2))

    case "R_Lubm_2" =>
      val atomSubOrganizationOf_XY = Atom(addPref(lubmPref, "subOrganizationOf"), Term("X"), Term("Y"))
      val atomBaseOrg = Atom(addTempPref("baseOrg"), Term("X"), Term("Y"))
      val atomUpdateSubOrg_XY = Atom(addTempPref("updateSubOrg"), Term("X"), Term("Y"))
      val atomUpdateSubOrg_XZ = Atom(addTempPref("updateSubOrg"), Term("X"), Term("Z"))
      val atomUpdateSubOrg_YZ = Atom(addTempPref("updateSubOrg"), Term("Y"), Term("Z"))
      val atomUpdateSubOrg_AB = Atom(addTempPref("updateSubOrg"), Term("A"), Term("B"))
      val atomResult = Atom(resIRI, Term("A"), Term("B"))
      //--------------------- Recursive Part ---------------------
      val rule6 = Rule(atomBaseOrg, Set(atomSubOrganizationOf_XY), f_win(range, slide))
      val rule7 = Rule(atomUpdateSubOrg_XY, Set(atomBaseOrg), f_win(range, slide))
      val rule8 = Rule(atomUpdateSubOrg_XZ, Set(atomUpdateSubOrg_XY, atomUpdateSubOrg_YZ), f_win(range, slide))
      val rule9 = Rule(atomResult, Set(atomUpdateSubOrg_AB), f_win(range, slide))
      //----------------------------------------------------------
      Program(Set(rule6, rule7, rule8, rule9))

    case "R_Lubm_3" =>
      val atom1_X = Atom(addPref(rdfSyntaxPref, "type"), Term("X"), Term(addPref(lubmPref, "GraduateStudent")))
      val atom2_XY = Atom(addPref(lubmPref, "memberOf"), Term("X"), Term("Y"))
      val atom3_SY = Atom(addPref(lubmPref, "undergraduateDegreeFrom"), Term("S"), Term("Y"))
      val atom4_X = Atom(addPref(rdfSyntaxPref, "type"), Term("X"), Term(addPref(lubmPref, "University")))
      val atom5_X = Atom(addPref(rdfSyntaxPref, "type"), Term("X"), Term("Department"))
      val atom6_XY = Atom(addPref(lubmPref, "subOrganizationOf"), Term("X"), Term("Y"))
      val atom6_YZ = Atom(addPref(lubmPref, "subOrganizationOf"), Term("Y"), Term("Z"))

      val atomGraduateStudent = Atom(addTempPref("graduateStudent"), Term("X"))
      val atomMemberOf_YX = Atom(addTempPref("memberOf"), Term("Y"), Term("X"))
      val atomMemberOf_XZ = Atom(addTempPref("memberOf"), Term("X"), Term("Z"))
      val atomUgDegreeFrom_YX = Atom(addTempPref("ugDegreeFrom"), Term("Y"), Term("X"))
      val atomUgDegreeFrom_XY = Atom(addTempPref("ugDegreeFrom"), Term("X"), Term("Y"))
      val atomUniv = Atom(addTempPref("univ"), Term("X"))
      val atomUniv_Y = Atom(addTempPref("univ"), Term("Y"))
      val atomDept = Atom(addTempPref("dept"), Term("X"))
      val atomDept_Z = Atom(addTempPref("dept"), Term("Z"))
      val atomBaseOrg = Atom(addTempPref("baseOrg"), Term("X"), Term("Y"))
      val atomUpdateSubOrg_XY = Atom(addTempPref("updateSubOrg"), Term("X"), Term("Y"))
      val atomUpdateSubOrg_XZ = Atom(addTempPref("updateSubOrg"), Term("X"), Term("Z"))
      val atomUpdateSubOrg_YZ = Atom(addTempPref("updateSubOrg"), Term("Y"), Term("Z"))
      val atomTemp1 = Atom(addTempPref("temp1"), Term("X"), Term("Y"))
      val atomTemp2 = Atom(addTempPref("temp2"), Term("Y"))
      val atomResult = Atom(resIRI, Term("X"), Term("Y"))

      val rule1 = Rule(atomGraduateStudent, Set(atom1_X), f_win(range, slide))
      val rule2 = Rule(atomMemberOf_YX, Set(atom2_XY), f_win(range, slide))
      val rule3 = Rule(atomUgDegreeFrom_YX, Set(atom3_SY), f_win(range, slide))
      val rule4 = Rule(atomUniv, Set(atom4_X), f_win(range, slide))
      val rule5 = Rule(atomDept, Set(atom5_X), f_win(range, slide))
      //--------------------- Recursive Part ---------------------
      val rule6 = Rule(atomBaseOrg, Set(atom6_XY))
      val rule7 = Rule(atomUpdateSubOrg_XY, Set(atomBaseOrg), f_win(range, slide))
      val rule8 = Rule(atomUpdateSubOrg_XZ, Set(atomUpdateSubOrg_XY, atomUpdateSubOrg_YZ), f_win(range, slide))
      //----------------------------------------------------------
      val rule9 = Rule(atomTemp1, Set(atomGraduateStudent, atomMemberOf_XZ, atomUgDegreeFrom_XY), f_win(range, slide))
      val rule10 = Rule(atomTemp2, Set(atom6_YZ, atomUniv_Y, atomDept_Z), f_win(range, slide))
      val rule11 = Rule(atomResult, Set(atomTemp1, atomTemp2), f_win(range, slide))
      Program(Set(rule1, rule2, rule3, rule4, rule5, rule6, rule7, rule8, rule9, rule10, rule11))

    //------------------------------ Stateless ------------------------------
    //-----------------------------------------------------------------------
    case "Stateless_Waves" =>
      val atom_tp1 = Atom(addPref(qudtPref, "numericValue"), Term("S"), Term("O"))
      val atom_res = Atom(resIRI, Term("S"), Term("O"))
      val rule = Rule(atom_res, Set(atom_tp1))
      Program(Set(rule))

    case "Stateless_SRBench" =>
      val atom = Atom(addPref(obsPref, "procedure"), Term("Obs"), Term("Sen"))
      val atom_result = Atom(resIRI, Term("Obs"), Term("Sen"))
      val rule = Rule(atom_result, Set(atom))
      Program(Set(rule))

    case "Stateless_CityBench" =>
      val atom = Atom(addPref(ssnPref1, "observedBy"), Term("ObId"),
        Term(addPref(servicePref, "AarhusTrafficData182955")))
      val atom_res = Atom(resIRI, Term("ObId"))
      val rule = Rule(atom_res, Set(atom))
      Program(Set(rule))

    case "Stateless_Lubm" =>
      val atom1 = Atom(addPref(lubmPref, "publicationAuthor"), Term("Pub"), Term("Author"))
      val atom_res = Atom(resIRI, Term("Pub"), Term("Author"))
      val rule = Rule(atom_res, Set(atom1))
      Program(Set(rule))

  }

}
