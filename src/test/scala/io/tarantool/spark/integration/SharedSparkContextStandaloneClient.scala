package io.tarantool.spark.integration

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

/** Shares a local `SparkContext` between all tests cases */
trait SharedSparkContextStandaloneClient extends BeforeAndAfterAll { self: Suite =>

  private val master = "local"
  private val appName = "tarantool-spark-test"

  @transient private var _sc: SparkContext = _
  def sc: SparkContext = _sc
  val conf: SparkConf = new SparkConf(false)
    .setMaster(master)
    .setAppName(appName)
    conf.set("tarantool.username", "guest")
    conf.set("tarantool.password", "")

  override def beforeAll() {
    super.beforeAll()
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
