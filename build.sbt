import sbt.Keys._

ThisBuild / name := "cartridge-spark"
ThisBuild / version := "1.0.0-SNAPSHOT"
ThisBuild / description := "Spark Connector for Tarantool and Tarantool Cartridge"
ThisBuild / organization := "io.tarantool"
ThisBuild / organizationName := "Tarantool"
ThisBuild / organizationHomepage := Some(url("https://www.tarantool.io"))
ThisBuild / licenses := Seq(
  "The 2-Clause BSD License" -> new URL(
    "https://opensource.org/licenses/BSD-2-Clause"))

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/tarantool/cartridge-spark"),
    "scm:git@github.com:tarantool/cartridge-spark.git"
  )
)
ThisBuild / developers := List(
  Developer(
    id    = "akudiyar",
    name  = "Alexey Kuzin",
    email = "akudiyar@gmail.com",
    url = url("https://tarantool.io/")
  )
)

ThisBuild / scalaVersion := "2.12.12"
ThisBuild / crossScalaVersions := Seq("2.11.12", "2.12.12", "2.13.3")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "io.netty.versions.properties") =>
    MergeStrategy.rename
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

lazy val root = (project in file("."))
  .settings(
    parallelExecution in Test := false,
    fork in Test := true,
    scalacOptions ++= Seq(
      "-unchecked",
      "-deprecation",
      "-language:_",
      "-target:jvm-1.8",
      "-encoding",
      "UTF-8"
    ),
    resolvers += Resolver.mavenLocal,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.0.1" % "provided",
      "org.apache.spark" %% "spark-sql" % "3.0.1" % "provided",
      "io.tarantool" % "cartridge-driver" % "0.3.2" % "provided",
      "org.apache.logging.log4j" % "log4j-api" % "2.2",
      "junit" % "junit" % "4.12" % "test",
      "org.scalatest" %% "scalatest" % "3.1.4" % "test",
      "com.dimafeng" %% "testcontainers-scala-scalatest" % "0.38.4" % "test",
      "io.tarantool" % "testcontainers-java-tarantool" % "0.2.0" % "test",
      "ch.qos.logback" % "logback-classic" % "1.2.3" % "test"
    ),
    dependencyOverrides ++= {
      Seq(
        "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.11.3",
        "com.fasterxml.jackson.core" % "jackson-databind" % "2.11.3",
        "com.fasterxml.jackson.core" % "jackson-core" % "2.11.3"
      )
    },
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases" at nexus + "service/local/staging/deploy/maven2")
    },
    publishMavenStyle := true,
  )
