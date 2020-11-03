package io.tarantool.spark.integration

import org.scalatest.{BeforeAndAfterAll, Suite}
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import io.tarantool.spark.containers.TarantoolCartridgeContainer

/**
  * @author Alexey Kuzin
  */
trait SharedCartridgeContainer
    extends BeforeAndAfterAll
    with TestContainerForAll { self: Suite =>
  override val containerDef = TarantoolCartridgeContainer.Def(
    instancesFile = "cartridge/instances.yml",
    topologyConfigurationFile = "cartridge/topology.lua",
    directoryResourcePath = "cartridge",
    routerPassword = "testapp-cluster-cookie"
  )

  override def beforeAll() {
    super.beforeAll()

    withContainers(container => {
      container.executeScript("test.lua").get()
    })
  }
}
