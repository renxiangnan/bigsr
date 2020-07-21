package org.bigsr.engine.core.spark.sparkeval.evaluator.op

import org.apache.spark.rdd.RDD
import org.bigsr.fwk.common.Fact
import org.bigsr.fwk.program.compiler.algebra._
import org.bigsr.fwk.program.formula.atom.Term


/**
  * @author xiangnan ren
  */
private[op]
class SparkSelection(override val relationName: String,
                     override val schema: Seq[Term])
  extends Selection(relationName, schema) {
  def eval(inputRDD: RDD[Fact]): RDD[Fact] = {
    if (filterInd.isEmpty) inputRDD
    else inputRDD.filter { fact =>
      filterInd.forall(i => fact(i) == schema(i).toString)
    }
  }
}