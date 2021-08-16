package io.tarantool.spark.connector.integration

import io.tarantool.spark.connector.containers.TarantoolCartridgeContainer
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

import java.util.concurrent.atomic.AtomicReference

/** Shares a local `SparkContext` between all tests cases */
trait SharedSparkContext extends BeforeAndAfterAll { self: Suite =>

  val container: TarantoolCartridgeContainer = new TarantoolCartridgeContainer(
    directoryBinding = "cartridge",
    instancesFile = "cartridge/instances.yml",
    topologyConfigurationFile = "cartridge/topology.lua",
    routerPassword = "testapp-cluster-cookie"
  )
  protected val sc: AtomicReference[SparkContext] = new AtomicReference[SparkContext]()
  private val master = "local"
  private val appName = "tarantool-spark-test"

  override def beforeAll(): Unit = {
    super.beforeAll()
    container.start()
    if (sc.get() == null) {
      sc.compareAndSet(null, new SparkContext(confWithTarantoolProperties(container.getRouterPort)))
    }
  }

  def confWithTarantoolProperties(routerPort: Int): SparkConf = {
    val _conf = new SparkConf(false)
      .setMaster(master)
      .setAppName(appName)
    _conf.set("tarantool.username", "admin")
    _conf.set("tarantool.password", "testapp-cluster-cookie")

    _conf.set("tarantool.hosts", "127.0.0.1:" + routerPort)

    _conf
  }

  override def afterAll(): Unit = {
    super.afterAll()
    val scRef = sc.get()
    scRef.stop()
    sc.compareAndSet(scRef, null)
    container.stop()
  }
}
