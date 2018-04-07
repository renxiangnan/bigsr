package spark.integration_test.launcher

import org.bigsr.engine.core.spark.sparkeval.evaluator.SparkProgramRunner
import org.bigsr.engine.core.spark.stream.SparkStreamUtils
import org.bigsr.program_examples.ExampleProgramBuilder
import spark.SharedSparkContext

/**
  * @author xiangnan ren
  */
object SparkStaticLauncherTest extends SharedSparkContext {

  def main(args: Array[String]): Unit = {
    val queryChoice = "R_Lubm_1"
    val outputIDB = "<http://big-sr/result>"
    val path = "/Users/xiangnanren/Desktop/EC2_Test_Suite/BigSREC2/data/lubm.nt"

    val inputRDD = spark.sparkContext.textFile(path).mapPartitions { iter =>
      for (i <- iter; a = i.split(" ", 4).take(3)) yield a.toSeq
    }

    inputRDD.cache()
    inputRDD.count()

    val inputRDDMap = SparkStreamUtils.convertSeqStream(ExampleProgramBuilder(queryChoice), inputRDD)
    val runner = SparkProgramRunner(ExampleProgramBuilder(queryChoice))
    (1 to 1) foreach { _ => runner.launch(inputRDDMap, spark, outputIDB) }

  }

}
