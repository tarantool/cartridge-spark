import sbt.Keys._

val scala211 = "2.11.12"
val scala212 = "2.12.14"
val supportedScalaVersions = Seq(scala212, scala211)

ThisBuild / description := "Spark Connector for Tarantool and Tarantool Cartridge"
ThisBuild / homepage := Some(url("https://github.com/tarantool/cartridge-spark"))
ThisBuild / organization := "io.tarantool"
ThisBuild / organizationName := "Tarantool"
ThisBuild / organizationHomepage := Some(url("https://www.tarantool.io"))

ThisBuild / licenses := Seq(
  "The 2-Clause BSD License" -> new URL("https://opensource.org/licenses/BSD-2-Clause")
)

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/tarantool/cartridge-spark"),
    "scm:git@github.com:tarantool/cartridge-spark.git"
  )
)

ThisBuild / developers := List(
  Developer(
    id = "akudiyar",
    name = "Alexey Kuzin",
    email = "akudiyar@gmail.com",
    url = url("https://tarantool.io/")
  )
)

ThisBuild / scalaVersion := scala211

val commonDependencies = Seq(
  "io.tarantool"     % "cartridge-driver"                % "0.10.0",
  "junit"            % "junit"                           % "4.12" % Test,
  "com.github.sbt"   % "junit-interface"                 % "0.12" % Test,
  "org.scalatest"    %% "scalatest"                      % "3.2.14" % Test,
  "org.scalamock"    %% "scalamock"                      % "5.2.0" % Test,
  "com.dimafeng"     %% "testcontainers-scala-scalatest" % "0.40.12" % Test,
  "ch.qos.logback"   % "logback-core"                    % "1.2.5" % Test,
  "ch.qos.logback"   % "logback-classic"                 % "1.2.5" % Test,
  "org.apache.derby" % "derby"                           % "10.11.1.1" % Test,
  "io.tarantool"     % "testcontainers-java-tarantool"   % "0.5.3" % Test
).map(
  _.exclude("io.netty", "netty-all")
    .exclude("io.netty", "netty-transport")
    .exclude("io.netty", "netty-handler")
    .exclude("io.netty", "netty-codec")
    .exclude("io.netty", "netty-codec-http")
    .exclude("org.slf4j", "slf4j-api")
)

lazy val root = (project in file("."))
  .settings(
    name := "spark-tarantool-connector",
    crossScalaVersions := supportedScalaVersions,
    // Dependencies
    libraryDependencies ++= (
      commonDependencies ++ Seq(
        "org.apache.spark" %% "spark-core" % "2.4.8" % "provided",
        "org.apache.spark" %% "spark-sql"  % "2.4.8" % "provided",
        "org.apache.spark" %% "spark-hive" % "2.4.8" % "provided"
      ).map(
        _.exclude("org.slf4j", "slf4j-log4j12")
      )
    ),
    dependencyOverrides ++= Seq(
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.7.1",
      "com.fasterxml.jackson.core"   % "jackson-databind"      % "2.6.7.3",
      "com.fasterxml.jackson.core"   % "jackson-core"          % "2.6.7",
      "io.netty"                     % "netty-all"             % "4.1.70.Final",
      "org.slf4j"                    % "slf4j-api"             % "1.7.36" % Test
    ),
    // Compiler options
    javacOptions ++= Seq(
      "-source",
      "1.8",
      "-target",
      "1.8"
    ),
    scalacOptions ++= Seq(
      "-unchecked",
      "-deprecation",
      "-language:_",
      "-target:jvm-1.8",
      "-encoding",
      "UTF-8"
    ),
    // Test frameworks options
    testOptions ++= Seq(
      Tests.Argument(TestFrameworks.JUnit, "-v"),
      Tests.Setup(() => System.setSecurityManager(null)) // SPARK-22918
    ),
    // Publishing settings
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (isSnapshot.value)
        Some("snapshots".at(nexus + "content/repositories/snapshots"))
      else
        Some("releases".at(nexus + "service/local/staging/deploy/maven2"))
    },
    publishMavenStyle := true,
    releaseUseGlobalVersion := false,
    releasePublishArtifactsAction := PgpKeys.publishSigned.value
  )

// Repositories
Global / resolvers += Resolver.mavenLocal

// This is a w/a for IJ IDEA not indexing the libraries for sbt 1.3.0+
ThisBuild / useCoursier := false
ThisBuild / updateSbtClassifiers / useCoursier := true

// Test settings
ThisBuild / Test / fork := true
ThisBuild / Test / parallelExecution := false
ThisBuild / Test / logLevel := Level.Info
ThisBuild / Test / javaOptions ++= Seq(
  "-DlogLevel=INFO"
)

// ScalaTest
ThisBuild / Test / logBuffered := false

// Settings for scoverage plug-in
// Disabled until this bug is fixed: https://github.com/scoverage/sbt-scoverage/issues/306
// ThisBuild / coverageEnabled := true
ThisBuild / coverageHighlighting := true
