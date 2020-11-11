package io.tarantool.spark.integration

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.Suite

/** Shares a local `SparkContext` between all tests cases */
trait SharedSparkContextClusterClient extends SharedCartridgeContainer {
  self: Suite =>

  private val master = "local"
  private val appName = "tarantool-spark-test"

  @transient private var _sc: SparkContext = _
  def sc: SparkContext = _sc
  val conf: SparkConf = new SparkConf(false)
    .setMaster(master)
    .setAppName(appName)

  override def beforeAll() {
    super.beforeAll()
    withContainers(container => {
      conf.set("tarantool.username", container.username)
      conf.set("tarantool.password", container.password)

      conf.set("tarantool.useProxyClient", "1")
      conf.set("tarantool.hosts",
               container.host + ":" + String.valueOf(container.port))

      conf.set("tarantool.discoveryProvider", "binary")
      conf.set("tarantool.discoveryBinaryEntryFunction", "get_s1_master")
      conf.set("tarantool.discoveryBinaryHost", container.host + ":" + String.valueOf(container.port))
    })
    _sc = new SparkContext(conf)
  }

  override def afterAll() {
    try {
      println("===afterAll===")
      _sc.stop()
      _sc = null
    } finally {
      super.afterAll()
    }
  }
}
