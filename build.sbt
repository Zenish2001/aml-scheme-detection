// AML Scheme Detection: a personal project by Zenish Borad
// Three modules, so Spark's and Akka's dependencies never clash.

ThisBuild / scalaVersion := "2.13.18"   // Spark needs Scala 2.13, as in the course's spark-csv module
ThisBuild / organization := "edu.neu.csye7200.aml"
ThisBuild / version      := "0.1.0-SNAPSHOT"

lazy val sparkVersion = "4.0.1"          // as in spark-csv
lazy val akkaVersion  = "2.8.8"          // as in lab-actors (Maven Central, no token needed)
lazy val scalaTest    = "org.scalatest" %% "scalatest" % "3.2.19" % Test

// Uncomment locally if sbt picks the wrong JDK (same fix as the Titanic assignment):
// ThisBuild / javaHome := Some(file("/Users/zenishborad/Library/Java/JavaVirtualMachines/ms-17.0.20.1/Contents/Home"))

lazy val sparkJvmFlags = Seq("-Xmx6g",
  "--add-opens=java.base/java.lang=ALL-UNNAMED", "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED", "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED", "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED", "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED", "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED")

/** Shared code with no Spark and no Akka: the rules DSL and the model-weights format. */
lazy val common = (project in file("common")).settings(
  name := "aml-common",
  libraryDependencies ++= Seq(
    "org.scala-lang.modules" %% "scala-parser-combinators" % "2.4.0",
    scalaTest))

/** Batch path: ingestion, features, models (Spark). */
lazy val batch = (project in file("batch")).dependsOn(common).settings(
  name := "aml-batch",
  libraryDependencies ++= Seq(
    "org.apache.spark"    %% "spark-sql"         % sparkVersion,
    "org.apache.spark"    %% "spark-mllib"       % sparkVersion,
    "org.apache.spark"    %% "spark-graphx"      % sparkVersion,
    "com.phasmidsoftware" %% "tableparser-spark" % "1.7.2",   // slide D2-43; fall back to 1.2.5 if it clashes with Spark 4.0.1
    scalaTest),
  fork := true,
  javaOptions ++= sparkJvmFlags,
  run / baseDirectory := (ThisBuild / baseDirectory).value,   // run from the repo root, so "data/raw/..." paths work
  outputStrategy := Some(StdoutOutput),                        // show the program's output as it is
  Test / fork := true,
  Test / javaOptions ++= sparkJvmFlags)

/** Real-time path: the live monitor (Akka typed actors and Streams). */
lazy val monitor = (project in file("monitor")).dependsOn(common).settings(
  name := "aml-monitor",
  libraryDependencies ++= Seq(
    "com.typesafe.akka" %% "akka-actor-typed"         % akkaVersion,
    "com.typesafe.akka" %% "akka-stream"              % akkaVersion,
    "ch.qos.logback"     % "logback-classic"          % "1.4.14",
    "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
    scalaTest))

lazy val root = (project in file(".")).aggregate(common, batch, monitor).settings(name := "aml-scheme-detection")
