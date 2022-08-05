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

  private lazy val warehouseLocation = Files.createTempDirectory("spark-wirehouse").toFile

  val container: TarantoolCartridgeContainer = new TarantoolCartridgeContainer(
    directoryBinding = "cartridge",
    instancesFile = "cartridge/instances.yml",
    topologyConfigurationFile = "cartridge/topology.lua",
    routerPassword = "testapp-cluster-cookie"
  )
  private val sparkSession: AtomicReference[SparkSession] = new AtomicReference[SparkSession]()
  private val master = "local"
  private val appName = "tarantool-spark-test"

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
      .config("spark.sql.warehouse.dir", warehouseLocationPath)
      .config(
        "javax.jdo.option.ConnectionURL",
        "jdbc:derby:;databaseName=tarantoolTest;create=true"
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
    _conf.set("tarantool.password", "testapp-cluster-cookie")
    _conf.set("tarantool.hosts", "127.0.0.1:" + routerPort)

    _conf
  }

  def sc: SparkContext =
    sparkSession.get().sparkContext

  def spark: SparkSession =
    sparkSession.get()

  def teardownSpark(): Unit = {
    val scRef = sparkSession.get()
    if (sparkSession.compareAndSet(scRef, null)) {
      scRef.stop()
    }
    cleanupTempDirectory()
  }

  def cleanupTempDirectory(): Unit =
    Directory(warehouseLocation).deleteRecursively()

  def teardown(): Unit =
    container.stop()
}
