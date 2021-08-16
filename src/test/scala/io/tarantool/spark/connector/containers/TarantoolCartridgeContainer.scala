package io.tarantool.spark.connector.containers

import com.dimafeng.testcontainers.{ContainerDef, SingleContainer}
import org.slf4j.{Logger, LoggerFactory}
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.containers.{
  TarantoolCartridgeContainer => JavaTarantoolCartridgeContainer
}

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
  routerPassword: String = TarantoolCartridgeContainer.defaultRouterPassword
) extends SingleContainer[JavaTarantoolCartridgeContainer] {

  lazy val container: JavaTarantoolCartridgeContainer =
    new JavaTarantoolCartridgeContainer(dockerFile, instancesFile, topologyConfigurationFile)
      .withLogConsumer(logConsumer)
      .withInstanceDir(instanceDir)
      .withDirectoryBinding(directoryBinding)
      .withRouterHost(routerHost)
      .withRouterPort(routerPort)
      .withAPIPort(apiPort)
      .withRouterUsername(routerUsername)
      .withRouterPassword(routerPassword)
  val logger: Logger = LoggerFactory.getLogger("tarantool")
  val logConsumer = new Slf4jLogConsumer(logger)

  def getRouterPort: Int = container.getRouterPort
  def getAPIPort: Int = container.getAPIPort

  def executeScript(scriptResourcePath: String): CompletableFuture[util.List[_]] =
    container.executeScript(scriptResourcePath)
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

  case class Def(
    dockerFile: String = TarantoolCartridgeContainer.defaultDockerFile,
    instancesFile: String = TarantoolCartridgeContainer.defaultInstancesFile,
    topologyConfigurationFile: String =
      TarantoolCartridgeContainer.defaultTopologyConfigurationFile,
    instanceDir: String = TarantoolCartridgeContainer.defaultInstanceDir,
    directoryBinding: String = TarantoolCartridgeContainer.defaultDirectoryBinding,
    routerHost: String = TarantoolCartridgeContainer.defaultRouterHost,
    routerPort: Int = TarantoolCartridgeContainer.defaultRouterPort,
    apiPort: Int = TarantoolCartridgeContainer.defaultAPIPort,
    routerUsername: String = TarantoolCartridgeContainer.defaultRouterUsername,
    routerPassword: String = TarantoolCartridgeContainer.defaultRouterPassword
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
        routerPassword
      )
  }
}