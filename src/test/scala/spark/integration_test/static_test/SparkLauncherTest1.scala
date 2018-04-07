package spark.integration_test.static_test

import org.bigsr.engine.core.spark.sparkeval.evaluator.{SparkProgramRunner, SparkRelation}
import org.bigsr.fwk.common.Utils
import spark.SharedSparkContext

/**
  * @author xiangnan ren
  */
object SparkLauncherTest1 extends SharedSparkContext {
  val edge = Array(
    Seq("1", "2"), Seq("1", "3"), Seq("2", "4"),
    Seq("2", "5"), Seq("3", "6"), Seq("3", "7"),
    Seq("6", "8"), Seq("6", "9"), Seq("8", "10"),
    Seq("8", "11"), Seq("9", "12"), Seq("9", "13"))
  val odd = Array(Seq("1"), Seq("3"), Seq("5"),
    Seq("7"), Seq("9"), Seq("11"), Seq("13"))
  val even = Array(Seq("2"), Seq("4"), Seq("6"),
    Seq("8"), Seq("10"), Seq("12"))

  def main(args: Array[String]): Unit = {
    val edgeRDD = spark.sparkContext.parallelize(edge).cache()
    edgeRDD.count()
    val oddRDD = spark.sparkContext.parallelize(odd).cache
    oddRDD.count()
    val evenRDD = spark.sparkContext.parallelize(even).cache
    evenRDD.count()
    val edbRDDMap = Map("E" -> SparkRelation(edgeRDD),
      "Odd" -> SparkRelation(oddRDD), "Even" -> SparkRelation(evenRDD))
    val runner = SparkProgramRunner(getProgram("p_1"))

    (1 to 100).foreach ( _ =>
      Utils.displayExecutionTime {
        runner.launch(edbRDDMap, spark, "T")}
    )

    Thread.sleep(3600000)
  }
}



