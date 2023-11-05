import sbt.Keys.*
import sbt.*

import scala.language.postfixOps

val scalaVersion_2_13 = "2.13.10"
val scalaVersion_3_00 = "3.0.1"

val appVersion = "0.1.6.0"
val pluginVersion = "1.0.0"
val scalaAppVersion = scalaVersion_2_13

val akkaVersion = "2.8.2"
val akkaHttpVersion = "10.5.2"
val awsKinesisClientVersion = "1.14.10"
val awsSDKVersion = "1.11.946"
val commonsIOVersion = "2.11.0"
val jekaVersion = "0.10.20"
val liftJsonVersion = "3.4.3"
val log4jVersion = "1.2.17"
val scalaJsIoVersion = "0.7.0"
val scalaTestVersion = "3.3.0-SNAP3"
val slf4jVersion = "2.0.5"
val snappyJavaVersion = "1.1.9.1"

lazy val testDependencies = Seq(
  libraryDependencies ++= Seq(
    "log4j" % "log4j" % log4jVersion,
    "org.slf4j" % "slf4j-api" % slf4jVersion,
    "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
    "org.scalatest" %% "scalatest" % scalaTestVersion % Test
  ))

/////////////////////////////////////////////////////////////////////////////////
//      Root Project - builds all artifacts
/////////////////////////////////////////////////////////////////////////////////

/**
 * @example sbt "project root" package
 * @example sbt "project root" test
 */
lazy val root = (project in file("./app")).
  aggregate(core, jdbc_driver).
  dependsOn(core, jdbc_driver, examples).
  settings(testDependencies *).
  settings(
    name := "lollypop",
    organization := "com.lollypop",
    description := "Lollypop",
    version := appVersion,
    scalaVersion := scalaAppVersion,
    Compile / console / scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:implicitConversions", "-Xlint"),
    Compile / doc / scalacOptions += "-no-link-warnings",
    autoCompilerPlugins := true
  )

/////////////////////////////////////////////////////////////////////////////////
//      Core Project
/////////////////////////////////////////////////////////////////////////////////

/**
 * @example sbt "project core" assembly
 * @example sbt "project core" package
 * @example sbt "project core" test
 */
lazy val core = (project in file("./app/core")).
  settings(testDependencies *).
  settings(
    name := "core",
    organization := "com.lollypop",
    description := "Lollypop core language, run-time and utilities",
    version := appVersion,
    scalaVersion := scalaAppVersion,
    Compile / console / scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:implicitConversions", "-Xlint"),
    Compile / doc / scalacOptions += "-no-link-warnings",
    autoCompilerPlugins := true,
    assembly / mainClass := Some("com.lollypop.repl.LollypopCLI"),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case PathList("org", "apache", _*) => MergeStrategy.first
      case PathList("akka-http-version.conf") => MergeStrategy.concat
      case PathList("reference.conf") => MergeStrategy.concat
      case PathList("version.conf") => MergeStrategy.concat
      case _ => MergeStrategy.first
    },
    assembly / test := {},
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-discovery" % akkaVersion,
      "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,
      "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpVersion % Test,
      "commons-io" % "commons-io" % commonsIOVersion,
      "commons-codec" % "commons-codec" % "1.15",
      "dev.jeka" % "jeka-core" % jekaVersion,
      "org.apache.commons" % "commons-math3" % "3.6.1",
      "org.apache.httpcomponents.client5" % "httpclient5" % "5.2.1",
      "org.apache.httpcomponents.client5" % "httpclient5-fluent" % "5.2.1",
      "org.commonmark" % "commonmark" % "0.21.0",
      "org.jfree" % "jfreechart" % "1.5.4",
      "org.jfree" % "jfreechart-fx" % "1.0.1",
      "org.jfree" % "jcommon" % "1.0.24",
      "org.ow2.asm" % "asm" % "9.4",
      "org.scala-lang" % "scala-reflect" % scalaAppVersion,
      "org.xerial.snappy" % "snappy-java" % snappyJavaVersion
    ))

/////////////////////////////////////////////////////////////////////////////////
//      JDBC Driver Project
/////////////////////////////////////////////////////////////////////////////////

/**
 * @example sbt "project jdbc_driver" test
 */
lazy val jdbc_driver = (project in file("./app/jdbc-driver")).
  dependsOn(core).
  settings(testDependencies *).
  settings(
    name := "jdbc-driver",
    organization := "com.lollypop.database.jdbc",
    description := "Lollypop JDBC Driver",
    version := appVersion,
    scalaVersion := scalaAppVersion,
    Compile / console / scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:implicitConversions", "-Xlint"),
    Compile / doc / scalacOptions += "-no-link-warnings",
    autoCompilerPlugins := true,
    assembly / mainClass := Some("com.lollypop.database.jdbc.LollypopNetworkClient"),
    assembly / test := {},
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case PathList("org", "apache", _*) => MergeStrategy.first
      case PathList("akka-http-version.conf") => MergeStrategy.concat
      case PathList("reference.conf") => MergeStrategy.concat
      case PathList("version.conf") => MergeStrategy.concat
      case _ => MergeStrategy.first
    },
    libraryDependencies ++= Seq(

    ))

/////////////////////////////////////////////////////////////////////////////////
//      Examples Project
/////////////////////////////////////////////////////////////////////////////////

/**
 * @example sbt "project examples" test
 */
lazy val examples = (project in file("./app/examples")).
  dependsOn(jdbc_driver).
  settings(testDependencies *).
  settings(
    name := "examples",
    organization := "com.lollypop.examples",
    description := "Lollypop Examples",
    version := appVersion,
    scalaVersion := scalaAppVersion,
    Compile / console / scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:implicitConversions", "-Xlint"),
    Compile / doc / scalacOptions += "-no-link-warnings",
    autoCompilerPlugins := true)

// loads the Scalajs-io root project at sbt startup
onLoad in Global := (Command.process("project root", _: State)) compose (onLoad in Global).value
