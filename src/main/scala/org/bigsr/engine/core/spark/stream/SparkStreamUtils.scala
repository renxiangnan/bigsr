package org.bigsr.engine.core.spark.stream

import org.apache.spark.rdd.RDD
import org.bigsr.engine.core.spark.common.SparkEDBMap
import org.bigsr.engine.core.spark.sparkeval.evaluator.SparkRelation
import org.bigsr.fwk.common.Fact
import org.bigsr.fwk.program.Program
import org.bigsr.fwk.rdf.RDFTriple

/**
  * @author xiangnan ren
  */
object SparkStreamUtils {
  def convertTripleStream(program: Program,
                          inputRDD: RDD[RDFTriple],
                          withPredicate: Boolean = false): SparkEDBMap = {
    program.getEDBPredicates.map(predicate => predicate -> {
      val _rdd = inputRDD.mapPartitions(iter =>
        for (i <- iter if predicate == i.predicate) yield {
          if (withPredicate) Seq(i.subject, i.predicate, i.`object`)
          else Seq(i.subject, i.`object`)
        }).cache()

      SparkRelation(_rdd)
    }).toMap
  }

  def convertSeqStream(program: Program,
                       inputRDD: RDD[Seq[String]],
                       withPredicate: Boolean = false): SparkEDBMap = {
    program.getEDBPredicates.map(predicate => predicate -> {
      val _rdd = inputRDD.mapPartitions(iter =>
        for (i <- iter if predicate == i(1)) yield {
          if (withPredicate) i else i.patch(1, Nil, 1)
        }).cache()

      SparkRelation(_rdd)
    }).toMap
  }

  def displayResultRDD(inputRDD: RDD[Fact],
                       all: Boolean): Unit = {
    {
      if (all) inputRDD.collect()
      else inputRDD.take(50)
    }.foreach(f => println(s"${f.mkString(" ")}"))

  }
}
