name := "bigsr"

version := "0.1"

scalaVersion in ThisBuild := "2.11.7"
ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }


val sparkVersion = "2.2.1"
val flinkVersion = "1.4.0"

lazy val basicDependencies = Seq(
  "junit" % "junit" % "4.10" % "test",
  "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test"
)

lazy val sparkDependencies = Seq(
  "org.apache.spark" % "spark-core_2.11" % sparkVersion,
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion,
  "org.apache.spark" % "spark-streaming_2.11"  % sparkVersion,
  "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % sparkVersion
)

lazy val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
  "org.apache.flink" %% "flink-scala" % flinkVersion,
  "org.apache.flink" %% "flink-connector-kafka-0.9" % flinkVersion,
  "org.apache.flink" %% "flink-connector-filesystem" % flinkVersion,
  "org.apache.flink" %% "flink-runtime-web" % flinkVersion % Test
)

libraryDependencies ++= {
  basicDependencies ++ sparkDependencies ++ flinkDependencies
}

mainClass in assembly := some("org.bigsr.engine.core.spark.SparkLauncher")
test in assembly := {}
assemblyJarName := s"big-sr-spark.jar"
unmanagedResourceDirectories in Compile <++= baseDirectory { base =>
  Seq( base / "src/main/resources" )
}

assemblyMergeStrategy in assembly := {
  case x if x.endsWith(".class") => MergeStrategy.first
  case x if x.endsWith(".properties") => MergeStrategy.first
  case x if x.contains("/resources/") => MergeStrategy.first
  case x if x.startsWith("META-INF/mailcap") => MergeStrategy.first
  case x if x.startsWith("META-INF/mimetypes.default") => MergeStrategy.first
  case x if x.startsWith("META-INF/maven/org.slf4j/slf4j-api/pom.") => MergeStrategy.first
  case x if x.startsWith("META-INF/native/libnetty-transport-native-epoll.") => MergeStrategy.first
  case x if x.contains("overview.html") => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    if (oldStrategy == MergeStrategy.deduplicate)
      MergeStrategy.first
    else oldStrategy(x)
}

assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter {_.data.getName == "avro-ipc-1.7.7-tests.jar"}
}