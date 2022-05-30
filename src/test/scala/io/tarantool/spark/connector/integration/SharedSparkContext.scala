package io.tarantool.spark.connector.integration

import io.tarantool.spark.connector.containers.TarantoolCartridgeContainer
import org.apache.spark.sql.{SQLImplicits, SparkSession}
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
  private val sparkSession: AtomicReference[SparkSession] = new AtomicReference[SparkSession]()
  private val master = "local"
  private val appName = "tarantool-spark-test"

  override def beforeAll(): Unit = {
    super.beforeAll()
    container.start()
    if (sparkSession.get() == null) {
      sparkSession.compareAndSet(
        null,
        configureSparkSession(
          SparkSession.builder(),
          confWithTarantoolProperties(container.getRouterPort)
        ).getOrCreate()
      )
    }
  }

  def configureSparkSession(
    sessionBuilder: SparkSession.Builder,
    conf: SparkConf
  ): SparkSession.Builder = {
    sessionBuilder.config(conf).enableHiveSupport()
    sessionBuilder
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

  def sc: SparkContext =
    sparkSession.get().sparkContext

  def spark: SparkSession =
    sparkSession.get()

  protected lazy val sqlImplicits: SQLImplicits = spark.implicits

  override def afterAll(): Unit = {
    super.afterAll()
    try {
      val scRef = sparkSession.get()
      if (sparkSession.compareAndSet(scRef, null)) {
        scRef.stop()
      }
    } finally {
      container.stop()
    }
  }
}
