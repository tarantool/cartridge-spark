package io.tarantool.spark.connector.integration

import io.tarantool.spark.connector.Logging
import io.tarantool.spark.connector.containers.TarantoolCartridgeContainer
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import java.nio.file.Files
import java.util.concurrent.atomic.AtomicReference
import scala.reflect.io.Directory

/** Shared Docker container and Spark instance between all tests cases */
object SharedSparkContext extends Logging {

  private lazy val warehouseLocation = Files.createTempDirectory("spark-warehouse").toFile

  private lazy val clusterCookie =
    sys.env.getOrElse("TARANTOOL_CLUSTER_COOKIE", "testapp-cluster-cookie")

  private lazy val instancesFileName =
    sys.env.getOrElse("TARANTOOL_INSTANCES_FILE", "instances.yml")

  private lazy val buildArgs = Map(
    ("TARANTOOL_CLUSTER_COOKIE", clusterCookie),
    ("TARANTOOL_INSTANCES_FILE", instancesFileName)
  )

  private lazy val topologyFileName =
    sys.env.getOrElse("TARANTOOL_TOPOLOGY_FILE", "cartridge/topology.lua")

  private lazy val routerPort =
    sys.env.getOrElse("TARANTOOL_ROUTER_PORT", "3301")

  private lazy val apiPort =
    sys.env.getOrElse("TARANTOOL_ROUTER_API_PORT", "8081")

  val container: TarantoolCartridgeContainer = new TarantoolCartridgeContainer(
    directoryBinding = "cartridge",
    instancesFile = "cartridge/" + instancesFileName,
    topologyConfigurationFile = topologyFileName,
    routerPassword = clusterCookie,
    routerPort = Integer.valueOf(routerPort),
    apiPort = Integer.valueOf(apiPort),
    buildArgs = buildArgs
  )
  private val sparkSession: AtomicReference[SparkSession] = new AtomicReference[SparkSession]()
  private val master = "local"
  private val appName = "tarantool-spark-test"

  private val CONTAINER_STOP_DELAY = 10000 // ms

  def setup(): Unit =
    container.start()

  def setupSpark(withHiveSupport: Boolean = false): Unit =
    if (sparkSession.get() == null) {
      sparkSession.compareAndSet(
        null,
        configureSparkSession(
          SparkSession.builder(),
          confWithTarantoolProperties(container.getRouterPort),
          withHiveSupport
        ).getOrCreate()
      )
    }

  private def configureSparkSession(
    sessionBuilder: SparkSession.Builder,
    conf: SparkConf,
    withHiveSupport: Boolean = false
  ): SparkSession.Builder = {
    val warehouseLocationPath = warehouseLocation.getAbsolutePath
    var session = sessionBuilder
      .config(conf)
      .config("spark.ui.enabled", false)
      .config("spark.sql.warehouse.dir", warehouseLocationPath)
      .config("hive.metastore.warehouse.dir", warehouseLocationPath)
      .config(
        "javax.jdo.option.ConnectionURL",
        s"jdbc:derby:;databaseName=$warehouseLocationPath/tarantoolTest;create=true"
      )

    if (withHiveSupport)
      session = session.enableHiveSupport()

    session
  }

  private def confWithTarantoolProperties(routerPort: Int): SparkConf = {
    val _conf = new SparkConf(false)
      .setMaster(master)
      .setAppName(appName)
    _conf.set("tarantool.username", "admin")
    _conf.set("tarantool.password", clusterCookie)
    _conf.set("tarantool.hosts", "127.0.0.1:" + routerPort)

    _conf
  }

  def dbLocation: String =
    warehouseLocation.getAbsolutePath

  def sc: SparkContext =
    sparkSession.get().sparkContext

  def spark: SparkSession =
    sparkSession.get()

  def teardownSpark(): Unit = {
    val scRef = sparkSession.get()
    if (sparkSession.compareAndSet(scRef, null)) {
      scRef.stop()
    }
    Directory(warehouseLocation).deleteRecursively()
  }

  def teardown(): Unit = {
    container.stop()
    // This sleep is necessary for the container to finish freeing up
    // the resources like the network ports. Otherwise the tests starting
    // immediately after will encounter that the resources are not available.
    Thread.sleep(CONTAINER_STOP_DELAY)
  }
}
