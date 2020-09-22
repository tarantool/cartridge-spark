package io.tarantool.spark.integration

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

/** Shares a local `SparkContext` between all tests cases */
trait SharedSparkContextClusterClient extends BeforeAndAfterAll { self: Suite =>

  private val master = "local"
  private val appName = "tarantool-spark-test"

  @transient private var _sc: SparkContext = _
  def sc: SparkContext = _sc
  val conf: SparkConf = new SparkConf(false)
    .setMaster(master)
    .setAppName(appName)
    conf.set("tarantool.username", "admin")
    conf.set("tarantool.password", "myapp-cluster-cookie")

    conf.set("tarantool.isCluster", "1")
    conf.set("tarantool.hosts", "127.0.0.1:3301")

    //conf.set("tarantool.discoveryProvider", "binary")
    //conf.set("tarantool.discoveryBinaryEntryFunction", "get_routers_list")
    //conf.set("tarantool.discoveryBinaryHost", "127.0.0.1:3301")

    conf.set("tarantool.clusterSchemaFunction", "elect_cluster_api_get_schema")
    conf.set("tarantool.clusterFunctionPrefix", "elect_cluster_api")

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
