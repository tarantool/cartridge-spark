name := "tarantool-spark-connector"

organization := "io.tarantool.driver"

version := "2.2.0"

description := "Tarantool Spark Connector"

organizationHomepage := Some(url("https://www.tarantool.io"))

scalaVersion := "2.11.8"
crossScalaVersions := Seq("2.11.8", "2.10.6")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.2.0" % "provided",
  "io.tarantool" % "driver" % "1.0.0-SNAPSHOT",
  "org.scalatest" %% "scalatest" % "2.2.5" % "test",
  "junit" % "junit" % "4.12" % "test",
  "org.apache.logging.log4j" % "log4j-api" % "2.2"
)

dependencyOverrides ++= {
  Seq(
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.5",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5",
    "com.fasterxml.jackson.core" % "jackson-core" % "2.6.5"
  )
}

resolvers += Resolver.mavenLocal

licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.txt"))

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.rename
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
