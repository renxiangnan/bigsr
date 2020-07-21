package org.bigsr.engine.core.spark.sparkeval.conf

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


/**
  * @author xiangnan ren
  */
object SparkSingleton {

  @transient
  lazy val conf = new SparkConf().
    setAppName("Big-SR").
    set("spark.ui.enabled", "true").
    set("spark.locality.wait", "1s").
    set("spark.some.config.option", "some-value").
    set("spark.ui.showConsoleProgress", "false").
    set("spark.storage.memoryFraction", "0.5").
    set("spark.scheduler.mode", "FAIR").
    set("spark.streaming.backpressure.enabled", "false").
    set("spark.streaming.ui.retainedBatches", "300").
    set("spark.executor.extraJavaOptions", "-XX:+UseConcMarkSweepGC").
    set("spark.driver.extraJavaOptions", "-XX:+UseConcMarkSweepGC").
    set("spark.kryo.registrationRequired", "true").
    set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
    set("spark.kryo.registrator", classOf[SparkKryoRegistrator].getName)
  @transient
  lazy val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  var numPartition: Int = _
  var shuffledPartition: Int = _
  var checkpointThreshold: Int = _

  def init(localMode: Boolean,
           numPartition: Int = 8,
           shuffledPartition: Int = 4,
           concurrentJobs: Int = 8,
           checkpointThreshold: Int = 10): this.type = {
    if (localMode)
      this.conf.setMaster("local[*]").
        set("spark.driver.host", "localhost")
    this.conf.
      set("spark.default.parallelism", s"$numPartition").
      set("spark. streaming.concurrentJobs", s"$concurrentJobs")
    this.numPartition = numPartition
    this.shuffledPartition = shuffledPartition
    this.checkpointThreshold = checkpointThreshold
    this
  }
}
