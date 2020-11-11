package io.tarantool.spark.containers

import java.util.concurrent.CompletableFuture

import com.dimafeng.testcontainers.{ContainerDef, SingleContainer}
import org.testcontainers.containers.{TarantoolCartridgeContainer => JavaTarantoolCartridgeContainer}

/**
  * @author Alexey Kuzin
  */
case class TarantoolCartridgeContainer(instancesFile: String,
                                       topologyConfigurationFile: String,
                                       directoryResourcePath: String,
                                       routerPassword: String)
    extends SingleContainer[JavaTarantoolCartridgeContainer] {
  override val container: JavaTarantoolCartridgeContainer =
    new JavaTarantoolCartridgeContainer(instancesFile,
                                        topologyConfigurationFile)
      .withDirectoryBinding(directoryResourcePath)
      .withReuse(true)
      .withRouterPassword(routerPassword)

  def username: String = container.getRouterUsername

  def password: String = container.getRouterPassword

  override def host: String = container.getHost

  def port: Int = container.getPort

  def executeScript = container.executeScript _
}

object TarantoolCartridgeContainer {
  case class Def(instancesFile: String,
                 topologyConfigurationFile: String,
                 directoryResourcePath: String,
                 routerPassword: String)
      extends ContainerDef {

    override type Container = TarantoolCartridgeContainer

    override def createContainer(): TarantoolCartridgeContainer = {
      new TarantoolCartridgeContainer(instancesFile,
                                      topologyConfigurationFile,
                                      directoryResourcePath,
                                      routerPassword)
    }
  }
}
