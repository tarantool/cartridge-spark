import sbt.Keys._

name := "tarantool-spark-connector"

description := "Spark Connector for Tarantool and Tarantool Cartridge"

organization := "io.tarantool"

organizationName := "Tarantool"

organizationHomepage := Some(url("https://www.tarantool.io"))

licenses := Seq(
  "The 2-Clause BSD License" -> new URL("https://opensource.org/licenses/BSD-2-Clause")
)

scmInfo := Some(
  ScmInfo(
    url("https://github.com/tarantool/cartridge-spark"),
    "scm:git@github.com:tarantool/cartridge-spark.git"
  )
)

developers := List(
  Developer(
    id    = "akudiyar",
    name  = "Alexey Kuzin",
    email = "akudiyar@gmail.com",
    url = url("https://tarantool.io/")
  )
)

scalaVersion := "2.11.12"
crossScalaVersions := Seq("2.12.13", "2.11.12", "2.10.7")

val commonDependencies = Seq(
  "io.tarantool"             % "cartridge-driver"                  % "0.5.0",
  "org.apache.logging.log4j" % "log4j-api"                         % "2.2",
  "junit"                    % "junit"                             % "4.12"   % "test",
  "org.testcontainers"       % "testcontainers"                    % "1.15.3" % "test",
  "io.tarantool"             % "testcontainers-java-tarantool"     % "0.4.4"  % "test",
  "org.scalatest"            %% "scalatest"                        % "3.2.5"  % "test",
  "com.dimafeng"             %% "testcontainers-scala-scalatest"   % "0.39.4" % "test",
  "ch.qos.logback"           % "logback-classic"                   % "1.2.3"  % "test"
)

libraryDependencies ++= commonDependencies
libraryDependencies ++= {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, scalaMajor)) if scalaMajor >= 11 =>
      Seq(
        "org.apache.spark" %% "spark-core" % "2.4.8" % "provided",
        "org.apache.spark" %% "spark-sql"  % "2.4.8" % "provided",
      )
    case _ =>
      Seq(
        "org.apache.spark" %% "spark-core" % "2.2.3" % "provided",
        "org.apache.spark" %% "spark-sql"  % "2.2.3" % "provided"
      )
  }
}

dependencyOverrides ++= {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, scalaMajor)) if scalaMajor >= 11 =>
      Seq(
        "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.7",
        "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.3",
        "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7"
      )
    case _ =>
      Seq(
        "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.5",
        "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5",
        "com.fasterxml.jackson.core" % "jackson-core" % "2.6.5"
      )
  }
}

scalacOptions ++= Seq(
  "-unchecked",
  "-deprecation",
  "-language:_",
  "-target:jvm-1.8",
  "-encoding",
  "UTF-8"
)

resolvers += Resolver.mavenLocal

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

publishMavenStyle := true

// This is a w/a for IJ IDEA not indexing the libraries for sbt 1.3.0+
useCoursier := false
updateSbtClassifiers / useCoursier := true

Test / fork := false
Test / parallelExecution := false
