package spark.integration_test.static_test

import org.apache.spark.storage.StorageLevel
import org.bigsr.engine.core.spark.sparkeval.evaluator.{SparkProgramRunner, SparkRelation}
import spark.SharedSparkContext

/**
  * @author xiangnan ren
  */
object SparkLauncherTest2 extends SharedSparkContext {
  val r = Array(
    Seq("1", "2"), Seq("2", "1"), Seq("2", "3"),
    Seq("1", "4"), Seq("3", "4"), Seq("4", "5"))

  def main(args: Array[String]): Unit = {
    val rdd = spark.sparkContext.parallelize(r).persist(StorageLevel.MEMORY_ONLY)
    val edbRDDMap = Map("R" -> SparkRelation(rdd))
    rdd.count()
    val runner = SparkProgramRunner(getProgram("p_2"))

    (1 to 100).foreach ( _ => runner.launch(edbRDDMap, spark, "T"))

    Thread.sleep(3600000)
  }

}
