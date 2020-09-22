package io.tarantool.spark.connection

import io.tarantool.driver.TarantoolServerAddress
import io.tarantool.spark.partition.TarantoolPartitioner
import org.apache.spark.SparkConf

case class Credential(username: String, password: String)

case class Timeouts(connect: Option[Int], read: Option[Int], request: Option[Int])

case class ClusterDiscoveryTimeouts(connect: Option[Int], read: Option[Int], delay: Option[Int])

case class TarantoolClusterConfig(operationsMapping: ClusterOperationsMapping, discoveryConfig: Option[ClusterDiscoveryConfig])

case class ClusterDiscoveryConfig(provider: String, timeouts: ClusterDiscoveryTimeouts,
                                  httpDiscoveryConfig: Option[ClusterHttpDiscoveryConfig],
                                  binaryDiscoveryConfig: Option[ClusterBinaryDiscoveryConfig])

case class ClusterHttpDiscoveryConfig(url: String)

case class ClusterBinaryDiscoveryConfig(entryFunction: String, address: TarantoolServerAddress)

case class ClusterOperationsMapping(clusterSchemaFunc: String,
                                    clusterFunctionsPrefix: Option[String] = None,
                                    deleteFunctionName: Option[String] = None,
                                    insertFunctionName: Option[String] = None,
                                    replaceFunctionName: Option[String] = None,
                                    selectFunctionName: Option[String] = None,
                                    updateFunctionName: Option[String] = None,
                                    upsertFunctionName: Option[String] = None)

trait TarantoolConfig {
  def space: String

  def partitioner: TarantoolPartitioner

  def hosts: Seq[TarantoolServerAddress]

  def credential: Option[Credential]

  def timeouts: Timeouts

  def clusterConfig: Option[TarantoolClusterConfig]
}

object TarantoolConfigBuilder {

  private val SPARK_PREFIX = "spark."
  private val PREFIX = "tarantool."

  private val USERNAME = PREFIX + "username"
  private val PASSWORD = PREFIX + "password"

  private val CONNECT_TIMEOUT = PREFIX + "connectTimeout"
  private val READ_TIMEOUT = PREFIX + "readTimeout"
  private val REQUEST_TIMEOUT = PREFIX + "requestTimeout"
  private val HOSTS = PREFIX + "hosts"

  //use cluster client
  private val IS_CLUSTER = PREFIX + "isCluster"

  //cluster discovery provider
  private val CLUSTER_DISCOVERY_PROVIDER = PREFIX + "discoveryProvider"
  private val CLUSTER_DISCOVERY_CONNECT_TIMEOUT = PREFIX + "discoverConnectTimeout"
  private val CLUSTER_DISCOVERY_READ_TIMEOUT = PREFIX + "discoveryReadTimeout"
  private val CLUSTER_DISCOVERY_DELAY = PREFIX + "discoveryDelay"

  private val CLUSTER_DISCOVERY_HTTP_URL = PREFIX + "discoveryHttpUrl"

  private val CLUSTER_DISCOVERY_BINARY_ENTRY_FUNCTION = PREFIX + "discoveryBinaryEntryFunction"
  private val CLUSTER_DISCOVERY_BINARY_HOST = PREFIX + "discoveryBinaryHost"

  //cluster operations mapping
  private val CLUSTER_SCHEMA_FUNC_NAME = PREFIX + "clusterSchemaFunction"
  private val CLUSTER_FUNC_PREFIX = PREFIX + "clusterFunctionPrefix"

  private val DELETE_FUNC_NAME = PREFIX + "deleteFunctionName"
  private val INSERT_FUNC_NAME = PREFIX + "insertFunctionName"
  private val REPLACE_FUNC_NAME = PREFIX + "replaceFunctionName"
  private val SELECT_FUNC_NAME = PREFIX + "selectFunctionName"
  private val UPDATE_FUNC_NAME = PREFIX + "updateFunctionName"
  private val UPSERT_FUNC_NAME = PREFIX + "upsertFunctionName"

  //options with spark. prefix
  private val SPARK_USERNAME = SPARK_PREFIX + USERNAME
  private val SPARK_PASSWORD = SPARK_PREFIX + PASSWORD

  private val SPARK_CONNECT_TIMEOUT = SPARK_PREFIX + CONNECT_TIMEOUT
  private val SPARK_READ_TIMEOUT = SPARK_PREFIX + READ_TIMEOUT
  private val SPARK_REQUEST_TIMEOUT = SPARK_PREFIX + REQUEST_TIMEOUT
  private val SPARK_HOSTS = SPARK_PREFIX + HOSTS

  private val SPARK_IS_CLUSTER = SPARK_PREFIX + IS_CLUSTER

  private val SPARK_CLUSTER_DISCOVERY_PROVIDER = SPARK_PREFIX + CLUSTER_DISCOVERY_PROVIDER
  private val SPARK_CLUSTER_DISCOVERY_CONNECT_TIMEOUT = SPARK_PREFIX + CLUSTER_DISCOVERY_CONNECT_TIMEOUT
  private val SPARK_CLUSTER_DISCOVERY_READ_TIMEOUT = SPARK_PREFIX + CLUSTER_DISCOVERY_READ_TIMEOUT
  private val SPARK_CLUSTER_DISCOVERY_DELAY = SPARK_PREFIX + CLUSTER_DISCOVERY_DELAY

  private val SPARK_CLUSTER_DISCOVERY_HTTP_URL = SPARK_PREFIX + CLUSTER_DISCOVERY_HTTP_URL

  private val SPARK_CLUSTER_DISCOVERY_BINARY_ENTRY_FUNCTION = SPARK_PREFIX + CLUSTER_DISCOVERY_BINARY_ENTRY_FUNCTION
  private val SPARK_CLUSTER_DISCOVERY_BINARY_HOST = SPARK_PREFIX + CLUSTER_DISCOVERY_BINARY_HOST

  private val SPARK_CLUSTER_SCHEMA = SPARK_PREFIX + CLUSTER_SCHEMA_FUNC_NAME
  private val SPARK_CLUSTER_FUNC_PREFIX = SPARK_PREFIX + CLUSTER_FUNC_PREFIX

  private val SPARK_DELETE_FUNC_NAME = SPARK_PREFIX + DELETE_FUNC_NAME
  private val SPARK_INSERT_FUNC_NAME = SPARK_PREFIX + INSERT_FUNC_NAME
  private val SPARK_REPLACE_FUNC_NAME = SPARK_PREFIX + REPLACE_FUNC_NAME
  private val SPARK_SELECT_FUNC_NAME = SPARK_PREFIX + SELECT_FUNC_NAME
  private val SPARK_UPDATE_FUNC_NAME = SPARK_PREFIX + UPDATE_FUNC_NAME
  private val SPARK_UPSERT_FUNC_NAME = SPARK_PREFIX + UPSERT_FUNC_NAME

  def parseCredentials(cfg: SparkConf): Option[Credential] = {
    val username = cfg.getOption(USERNAME).orElse(cfg.getOption(SPARK_USERNAME))
    val password = cfg.getOption(PASSWORD).orElse(cfg.getOption(SPARK_PASSWORD))

    if (username.isDefined) {
      Some(Credential(username.get, password.get))
    } else {
      None
    }
  }

  def parseHosts(cfg: SparkConf): Seq[TarantoolServerAddress] = {
    var hosts = cfg.get(HOSTS, "").split(";")
      .union(cfg.get(SPARK_HOSTS, "").split(";"))
      .distinct
      .filter(!_.isEmpty)
      .map(a => new TarantoolServerAddress(a))

    if (hosts.isEmpty) {
      hosts = hosts ++ Array(new TarantoolServerAddress(TarantoolDefaults.DEFAULT_HOST))
    }
    hosts
  }

  def parseTimeouts(cfg: SparkConf): Timeouts = {
    Timeouts(
      parseTimeout(cfg, CONNECT_TIMEOUT, SPARK_CONNECT_TIMEOUT),
      parseTimeout(cfg, READ_TIMEOUT, SPARK_READ_TIMEOUT),
      parseTimeout(cfg, REQUEST_TIMEOUT, SPARK_REQUEST_TIMEOUT)
    )
  }

  def parseTimeout(cfg: SparkConf, name: String, nameWithSparkPrefix: String): Option[Int] = {
    cfg.getOption(name).orElse(cfg.getOption(nameWithSparkPrefix)).map(_.toInt)
  }

  def parseClusterMapping(cnf: SparkConf): ClusterOperationsMapping = {
    val funcPrefix = parseMappingValue(cnf, CLUSTER_FUNC_PREFIX, SPARK_CLUSTER_FUNC_PREFIX)

    ClusterOperationsMapping(
      clusterSchemaFunc = parseMappingValue(cnf, CLUSTER_SCHEMA_FUNC_NAME, SPARK_CLUSTER_SCHEMA, isRequire = true).get,
      clusterFunctionsPrefix = funcPrefix,
      deleteFunctionName = parseMappingValue(cnf, DELETE_FUNC_NAME, SPARK_DELETE_FUNC_NAME, funcPrefix.isEmpty),
      insertFunctionName = parseMappingValue(cnf, INSERT_FUNC_NAME, SPARK_INSERT_FUNC_NAME, funcPrefix.isEmpty),
      replaceFunctionName = parseMappingValue(cnf, REPLACE_FUNC_NAME, SPARK_REPLACE_FUNC_NAME, funcPrefix.isEmpty),
      selectFunctionName = parseMappingValue(cnf, SELECT_FUNC_NAME, SPARK_SELECT_FUNC_NAME, funcPrefix.isEmpty),
      updateFunctionName = parseMappingValue(cnf, UPDATE_FUNC_NAME, SPARK_UPDATE_FUNC_NAME, funcPrefix.isEmpty),
      upsertFunctionName = parseMappingValue(cnf, UPSERT_FUNC_NAME, SPARK_UPSERT_FUNC_NAME, funcPrefix.isEmpty)
    )
  }

  def parseMappingValue(cfg: SparkConf, name: String, nameWithSparkPrefix: String, isRequire: Boolean = false): Option[String] = {
    val value = cfg.getOption(name).orElse(cfg.getOption(nameWithSparkPrefix))
    if (isRequire) {
      require(value.isDefined, s"$name cannot be null")
    }
    value
  }

  def parseClusterConfig(cfg: SparkConf): Option[TarantoolClusterConfig] = {
    val clusterOperationsMapping = parseClusterMapping(cfg)

    val discoveryProvider = cfg.getOption(CLUSTER_DISCOVERY_PROVIDER).orElse(cfg.getOption(SPARK_CLUSTER_DISCOVERY_PROVIDER))

    val discoveryConfig = if (discoveryProvider.isDefined) {
      if (discoveryProvider.get != TarantoolDefaults.DISCOVERY_PROVIDER_HTTP &&
        discoveryProvider.get != TarantoolDefaults.DISCOVERY_PROVIDER_BINARY) {
        throw new IllegalArgumentException("Invalid DiscoveryProvider option value : \"" + discoveryProvider.get + "\". " +
          "Allowed values: " + TarantoolDefaults.DISCOVERY_PROVIDER_BINARY + ", " + TarantoolDefaults.DISCOVERY_PROVIDER_HTTP)
      }

      val httpDiscoveryConfig = parseHttpDiscoveryConfig(cfg)
      val binaryDiscoveryConfig = parseBinaryDiscoveryConfig(cfg)

      if (discoveryProvider.get == TarantoolDefaults.DISCOVERY_PROVIDER_HTTP && httpDiscoveryConfig.isEmpty) {
        throw new IllegalArgumentException("Invalid http discovery options")
      }

      if (discoveryProvider.get == TarantoolDefaults.DISCOVERY_PROVIDER_BINARY && binaryDiscoveryConfig.isEmpty) {
        throw new IllegalArgumentException("Invalid binary discovery options")
      }

      Some(ClusterDiscoveryConfig(
        discoveryProvider.get,
        parseDiscoveryTimeouts(cfg),
        httpDiscoveryConfig,
        binaryDiscoveryConfig
      ))
    } else {
      None
    }

    Some(TarantoolClusterConfig(clusterOperationsMapping, discoveryConfig))
  }

  def parseDiscoveryTimeouts(cfg: SparkConf): ClusterDiscoveryTimeouts = {
    ClusterDiscoveryTimeouts(
      parseTimeout(cfg, CLUSTER_DISCOVERY_CONNECT_TIMEOUT, SPARK_CLUSTER_DISCOVERY_CONNECT_TIMEOUT),
      parseTimeout(cfg, CLUSTER_DISCOVERY_READ_TIMEOUT, SPARK_CLUSTER_DISCOVERY_READ_TIMEOUT),
      parseTimeout(cfg, CLUSTER_DISCOVERY_DELAY, SPARK_CLUSTER_DISCOVERY_DELAY)
    )
  }

  def parseHttpDiscoveryConfig(cfg: SparkConf): Option[ClusterHttpDiscoveryConfig] = {
    val url = cfg.getOption(CLUSTER_DISCOVERY_HTTP_URL).orElse(cfg.getOption(SPARK_CLUSTER_DISCOVERY_HTTP_URL))
    if (url.isDefined) {
      Some(ClusterHttpDiscoveryConfig(url.get))
    } else {
      None
    }
  }

  def parseBinaryDiscoveryConfig(cfg: SparkConf): Option[ClusterBinaryDiscoveryConfig] = {
    val entryFunction = cfg.getOption(CLUSTER_DISCOVERY_BINARY_ENTRY_FUNCTION)
      .orElse(cfg.getOption(SPARK_CLUSTER_DISCOVERY_BINARY_ENTRY_FUNCTION))

    val host = cfg.getOption(CLUSTER_DISCOVERY_BINARY_HOST).orElse(cfg.getOption(SPARK_CLUSTER_DISCOVERY_BINARY_HOST))

    if (entryFunction.isDefined && host.isDefined) {
      Some(ClusterBinaryDiscoveryConfig(entryFunction.get, new TarantoolServerAddress(host.get)))
    } else {
      None
    }
  }

  def createReadOptions(space: String, cfg: SparkConf): ReadOptions = {
    val isCluster = cfg.getOption(IS_CLUSTER).orElse(cfg.getOption(SPARK_IS_CLUSTER))
    val clusterConfig = if (isCluster.isDefined && (isCluster.get == "true" || isCluster.get == "1")) {
      parseClusterConfig(cfg)
    } else {
      None
    }

    ReadOptions(space = space,
      hosts = parseHosts(cfg),
      credential = parseCredentials(cfg),
      timeouts = parseTimeouts(cfg),
      clusterConfig = clusterConfig)
  }

  def createReadOptions(space: String): ReadOptions = ReadOptions(
    space = space,
    hosts = Seq(new TarantoolServerAddress(TarantoolDefaults.DEFAULT_HOST))
  )
}
