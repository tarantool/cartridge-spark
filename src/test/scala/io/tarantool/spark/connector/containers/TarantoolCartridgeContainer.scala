package io.tarantool.spark.connector.containers

import com.dimafeng.testcontainers.{ContainerDef, SingleContainer}
import org.slf4j.{Logger, LoggerFactory}
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.containers.{TarantoolCartridgeContainer => JavaTarantoolCartridgeContainer}
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.containers.Container

import scala.collection.JavaConverters.mapAsJavaMapConverter

import java.time.Duration
import java.util
import java.util.concurrent.CompletableFuture

case class TarantoolCartridgeContainer(
  dockerFile: String = TarantoolCartridgeContainer.defaultDockerFile,
  instancesFile: String = TarantoolCartridgeContainer.defaultInstancesFile,
  topologyConfigurationFile: String = TarantoolCartridgeContainer.defaultTopologyConfigurationFile,
  instanceDir: String = TarantoolCartridgeContainer.defaultInstanceDir,
  directoryBinding: String = TarantoolCartridgeContainer.defaultDirectoryBinding,
  routerHost: String = TarantoolCartridgeContainer.defaultRouterHost,
  routerPort: Int = TarantoolCartridgeContainer.defaultRouterPort,
  apiPort: Int = TarantoolCartridgeContainer.defaultAPIPort,
  routerUsername: String = TarantoolCartridgeContainer.defaultRouterUsername,
  routerPassword: String = TarantoolCartridgeContainer.defaultRouterPassword,
  buildArgs: Map[String, String] = TarantoolCartridgeContainer.defaultBuildArgs
) extends SingleContainer[JavaTarantoolCartridgeContainer] {
  val buildImageName: String = "tarantool-spark-test"

  lazy val container: JavaTarantoolCartridgeContainer =
    new JavaTarantoolCartridgeContainer(
      dockerFile,
      buildImageName,
      instancesFile,
      topologyConfigurationFile,
      buildArgs.asJava
    ).withLogConsumer(logConsumer)
      .withEnv("TARANTOOL_CLUSTER_COOKIE", routerPassword)
      .withInstanceDir(instanceDir)
      .withDirectoryBinding(directoryBinding)
      .withRouterHost(routerHost)
      .withRouterPort(routerPort)
      .withAPIPort(apiPort)
      .withRouterUsername(routerUsername)
      .withRouterPassword(routerPassword)
      .waitingFor(Wait.forLogMessage(".*Listening HTTP on.*", 6))
      .withStartupTimeout(Duration.ofMinutes(10))

  val logger: Logger = LoggerFactory.getLogger("tarantool")
  val logConsumer = new Slf4jLogConsumer(logger)

  def getRouterPort: Int = container.getRouterPort
  def getAPIPort: Int = container.getAPIPort

  def executeScript(scriptResourcePath: String): Unit = {
    val result = container.executeScript(scriptResourcePath)
    if (result.getExitCode != 0) {
      throw new RuntimeException("Failed to execute script. Details:\n" + result.getStderr)
    }
  }
}

object TarantoolCartridgeContainer {
  val defaultDockerFile = "Dockerfile"
  val defaultInstancesFile = "instances.yml"
  val defaultTopologyConfigurationFile = "topology.lua"
  val defaultInstanceDir = "/app"
  val defaultDirectoryBinding = ""
  val defaultRouterHost = "localhost"
  val defaultRouterPort = 3301
  val defaultAPIPort = 8081
  val defaultRouterUsername = "admin"
  val defaultRouterPassword = "testapp-cluster-cookie"
  val defaultBuildArgs = Map[String, String]()

  case class Def(
    dockerFile: String = TarantoolCartridgeContainer.defaultDockerFile,
    instancesFile: String = TarantoolCartridgeContainer.defaultInstancesFile,
    topologyConfigurationFile: String = TarantoolCartridgeContainer.defaultTopologyConfigurationFile,
    instanceDir: String = TarantoolCartridgeContainer.defaultInstanceDir,
    directoryBinding: String = TarantoolCartridgeContainer.defaultDirectoryBinding,
    routerHost: String = TarantoolCartridgeContainer.defaultRouterHost,
    routerPort: Int = TarantoolCartridgeContainer.defaultRouterPort,
    apiPort: Int = TarantoolCartridgeContainer.defaultAPIPort,
    routerUsername: String = TarantoolCartridgeContainer.defaultRouterUsername,
    routerPassword: String = TarantoolCartridgeContainer.defaultRouterPassword,
    buildArgs: Map[String, String] = TarantoolCartridgeContainer.defaultBuildArgs
  ) extends ContainerDef {
    override type Container = TarantoolCartridgeContainer

    override def createContainer(): TarantoolCartridgeContainer =
      new TarantoolCartridgeContainer(
        dockerFile,
        instancesFile,
        topologyConfigurationFile,
        instanceDir,
        directoryBinding,
        routerHost,
        routerPort,
        apiPort,
        routerUsername,
        routerPassword,
        buildArgs
      )
  }
}
