name := "BigDataRampUp.SparkStreaming"

version := "1.0"

scalaVersion := "2.10.6"

assemblyJarName in assembly := "apps.jar"

test in assembly := {}

parallelExecution in Test := false

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.1" % Provided,
  "org.apache.spark" %% "spark-sql" % "1.6.1" % Provided,
  "org.apache.spark" %% "spark-streaming" % "1.6.1" % Provided,
  "org.apache.spark" %% "spark-streaming-kafka" % "1.6.1" % Provided,

  "org.elasticsearch" %% "elasticsearch-spark" % "2.3.2",
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.6.0-M2",
  "com.restfb" % "restfb" % "1.23.0",
  "com.databricks" %% "spark-csv" % "1.3.0",
  "com.typesafe" % "config" % "1.3.0",
  "eu.bitwalker" % "UserAgentUtils" % "1.19",
  "joda-time" % "joda-time" % "2.9.3",

  "org.scalatest" %% "scalatest" % "2.2.4" % Test,
  "com.holdenkarau" %% "spark-testing-base" % "1.6.1_0.3.3" % Test,
  "org.scalamock" %% "scalamock-scalatest-support" % "3.2.2" % Test
)

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))
runMain in Compile <<= Defaults.runMainTask(fullClasspath in Compile, runner in (Compile, run))
