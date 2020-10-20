package io.tarantool.spark.integration

import com.dimafeng.testcontainers.{ForAllTestContainer, TarantoolContainer}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

/** Shares a local `SparkContext` between all tests cases */
trait SharedSparkContextStandaloneClient extends BeforeAndAfterAll with ForAllTestContainer {
  self: Suite =>

  override val container: TarantoolContainer =
    TarantoolContainer(scriptFileName = "org/testcontainers/containers/server.lua")

  private val master = "local"
  private val appName = "tarantool-spark-test"

  @transient private var _sc: SparkContext = _

  def sc: SparkContext = _sc

  val conf: SparkConf = new SparkConf(false)
    .setMaster(master)
    .setAppName(appName)

  override def beforeAll() {
    super.beforeAll()

    conf.set("tarantool.username", container.getUsername)
    conf.set("tarantool.password", container.getPassword)
    conf.set("tarantool.hosts", container.getHost + ":" + container.getPort)

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
