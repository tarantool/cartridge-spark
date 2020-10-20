package com.dimafeng.testcontainers

import org.testcontainers.containers.{TarantoolContainer => JavaTarantoolContainer}

case class TarantoolContainer(dockerImageName: String = TarantoolContainer.defaultDockerImageName,
                              scriptFileName: String = TarantoolContainer.defaultScriptFileName)
  extends SingleContainer[JavaTarantoolContainer] {

  override val container: JavaTarantoolContainer = {
    val c = new JavaTarantoolContainer(dockerImageName)
      .withScriptFileName(scriptFileName)
    c
  }

  def getHost: String = container.getHost

  def getPort: Int = container.getPort

  def getUsername: String = container.getUsername

  def getPassword: String = container.getPassword
}

object TarantoolContainer {

  val defaultImage = "tarantool/tarantool"
  val defaultTag = "2.x-centos7"
  val defaultDockerImageName = s"$defaultImage:$defaultTag"
  val defaultScriptFileName = "org/testcontainers/containers/server.lua"

  case class Def() extends ContainerDef {
    override type Container = TarantoolContainer

    override def createContainer(): TarantoolContainer = {
      new TarantoolContainer(defaultDockerImageName)
    }
  }

}
