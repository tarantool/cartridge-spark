package io.tarantool.spark.connector.integration

import io.tarantool.spark.connector.containers.TarantoolCartridgeContainer
import io.tarantool.spark.connector.Logging
import org.apache.spark.sql.{SQLImplicits, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}
import scala.reflect.io.Directory

import java.io.File
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicReference
import org.junit.rules.TemporaryFolder

/** Shared Docker container and Spark instance between all tests cases */
object SharedSparkContext extends Logging {

  val container: TarantoolCartridgeContainer = new TarantoolCartridgeContainer(
    directoryBinding = "cartridge",
    instancesFile = "cartridge/instances.yml",
    topologyConfigurationFile = "cartridge/topology.lua",
    routerPassword = "testapp-cluster-cookie"
  )

  private val sparkSession: AtomicReference[SparkSession] = new AtomicReference[SparkSession]()
  private val master = "local"
  private val appName = "tarantool-spark-test"
  private lazy val warehouseLocation = Files.createTempDirectory("spark-wirehouse").toFile

  def setup(): Unit =
    container.start()

  def setupSpark(): Unit =
    if (sparkSession.get() == null) {
      sparkSession.compareAndSet(
        null,
        configureSparkSession(
          SparkSession.builder(),
          confWithTarantoolProperties(container.getRouterPort)
        ).getOrCreate()
      )
    }

  private def configureSparkSession(
    sessionBuilder: SparkSession.Builder,
    conf: SparkConf
  ): SparkSession.Builder =
    sessionBuilder
      .config(conf)
      .config("spark.sql.warehouse.dir", warehouseLocation.getAbsolutePath())
      .config(
        "javax.jdo.option.ConnectionURL",
        "jdbc:derby:;databaseName=%s;create=true".format(warehouseLocation.getAbsolutePath())
      )
      .enableHiveSupport()

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

  def teardown(): Unit =
    container.stop()

  def cleanupTempDirectory(): Unit =
    Directory(warehouseLocation).deleteRecursively()
}
