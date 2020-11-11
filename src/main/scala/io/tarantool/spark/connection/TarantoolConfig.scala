package io.tarantool.spark.connection

import io.tarantool.driver.TarantoolServerAddress
import io.tarantool.spark.partition.TarantoolPartitioner
import org.apache.spark.SparkConf

case class Credentials(username: String, password: String)

case class Timeouts(connect: Option[Int],
                    read: Option[Int],
                    request: Option[Int])

case class ClusterDiscoveryTimeouts(connect: Option[Int],
                                    read: Option[Int],
                                    delay: Option[Int])

case class ClusterDiscoveryConfig(
    provider: String,
    timeouts: ClusterDiscoveryTimeouts,
    httpDiscoveryConfig: Option[ClusterHttpDiscoveryConfig],
    binaryDiscoveryConfig: Option[ClusterBinaryDiscoveryConfig])

case class ClusterHttpDiscoveryConfig(url: String)

case class ClusterBinaryDiscoveryConfig(entryFunction: String,
                                        address: TarantoolServerAddress)

trait TarantoolConfig {
  def space: String

  def partitioner: TarantoolPartitioner

  def hosts: Seq[TarantoolServerAddress]

  def credentials: Option[Credentials]

  def timeouts: Timeouts

  def useProxyClient: Boolean

  def clusterDiscoveryConfig: Option[ClusterDiscoveryConfig]
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

  //use cluster discovery
  private val USE_CLUSTER_DISCOVERY = PREFIX + "useClusterDiscovery"

  //cluster discovery provider
  private val CLUSTER_DISCOVERY_PROVIDER = PREFIX + "discoveryProvider"
  private val CLUSTER_DISCOVERY_CONNECT_TIMEOUT = PREFIX + "discoverConnectTimeout"
  private val CLUSTER_DISCOVERY_READ_TIMEOUT = PREFIX + "discoveryReadTimeout"
  private val CLUSTER_DISCOVERY_DELAY = PREFIX + "discoveryDelay"

  private val CLUSTER_DISCOVERY_HTTP_URL = PREFIX + "discoveryHttpUrl"

  private val CLUSTER_DISCOVERY_BINARY_ENTRY_FUNCTION = PREFIX + "discoveryBinaryEntryFunction"
  private val CLUSTER_DISCOVERY_BINARY_HOST = PREFIX + "discoveryBinaryHost"

  //use proxy client (with tarantool/crud)
  private val USE_PROXY_CLIENT = PREFIX + "useProxyClient"

  //options with spark. prefix
  private val SPARK_USERNAME = SPARK_PREFIX + USERNAME
  private val SPARK_PASSWORD = SPARK_PREFIX + PASSWORD

  private val SPARK_CONNECT_TIMEOUT = SPARK_PREFIX + CONNECT_TIMEOUT
  private val SPARK_READ_TIMEOUT = SPARK_PREFIX + READ_TIMEOUT
  private val SPARK_REQUEST_TIMEOUT = SPARK_PREFIX + REQUEST_TIMEOUT
  private val SPARK_HOSTS = SPARK_PREFIX + HOSTS

  private val SPARK_USE_CLUSTER_DISCOVERY = SPARK_PREFIX + USE_CLUSTER_DISCOVERY

  private val SPARK_CLUSTER_DISCOVERY_PROVIDER = SPARK_PREFIX + CLUSTER_DISCOVERY_PROVIDER
  private val SPARK_CLUSTER_DISCOVERY_CONNECT_TIMEOUT = SPARK_PREFIX + CLUSTER_DISCOVERY_CONNECT_TIMEOUT
  private val SPARK_CLUSTER_DISCOVERY_READ_TIMEOUT = SPARK_PREFIX + CLUSTER_DISCOVERY_READ_TIMEOUT
  private val SPARK_CLUSTER_DISCOVERY_DELAY = SPARK_PREFIX + CLUSTER_DISCOVERY_DELAY

  private val SPARK_CLUSTER_DISCOVERY_HTTP_URL = SPARK_PREFIX + CLUSTER_DISCOVERY_HTTP_URL

  private val SPARK_CLUSTER_DISCOVERY_BINARY_ENTRY_FUNCTION = SPARK_PREFIX + CLUSTER_DISCOVERY_BINARY_ENTRY_FUNCTION
  private val SPARK_CLUSTER_DISCOVERY_BINARY_HOST = SPARK_PREFIX + CLUSTER_DISCOVERY_BINARY_HOST

  private val SPARK_USE_PROXY_CLIENT = SPARK_PREFIX + USE_PROXY_CLIENT

  def parseCredentials(cfg: SparkConf): Option[Credentials] = {
    val username = cfg.getOption(USERNAME).orElse(cfg.getOption(SPARK_USERNAME))
    val password = cfg.getOption(PASSWORD).orElse(cfg.getOption(SPARK_PASSWORD))

    if (username.isDefined) {
      Some(Credentials(username.get, password.get))
    } else {
      None
    }
  }

  def parseHosts(cfg: SparkConf): Seq[TarantoolServerAddress] = {
    var hosts = cfg
      .get(HOSTS, "")
      .split(",")
      .union(cfg.get(SPARK_HOSTS, "").split(","))
      .distinct
      .filter(!_.isEmpty)
      .map(a => new TarantoolServerAddress(a))

    if (hosts.isEmpty) {
      hosts = hosts ++ Array(
        new TarantoolServerAddress(TarantoolDefaults.DEFAULT_HOST))
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

  def parseTimeout(cfg: SparkConf,
                   name: String,
                   nameWithSparkPrefix: String): Option[Int] = {
    cfg.getOption(name).orElse(cfg.getOption(nameWithSparkPrefix)).map(_.toInt)
  }

  def parseClusterDiscoveryConfig(
      cfg: SparkConf): Option[ClusterDiscoveryConfig] = {
    val discoveryProvider = cfg
      .getOption(CLUSTER_DISCOVERY_PROVIDER)
      .orElse(cfg.getOption(SPARK_CLUSTER_DISCOVERY_PROVIDER))

    if (discoveryProvider.isDefined) {
      if (discoveryProvider.get != TarantoolDefaults.DISCOVERY_PROVIDER_HTTP &&
          discoveryProvider.get != TarantoolDefaults.DISCOVERY_PROVIDER_BINARY) {
        throw new IllegalArgumentException(
          "Invalid DiscoveryProvider option value : \"" + discoveryProvider.get + "\". " +
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

      Some(
        ClusterDiscoveryConfig(
          discoveryProvider.get,
          parseDiscoveryTimeouts(cfg),
          httpDiscoveryConfig,
          binaryDiscoveryConfig
        ))
    } else {
      None
    }
  }

  def parseDiscoveryTimeouts(cfg: SparkConf): ClusterDiscoveryTimeouts = {
    ClusterDiscoveryTimeouts(
      parseTimeout(cfg,
                   CLUSTER_DISCOVERY_CONNECT_TIMEOUT,
                   SPARK_CLUSTER_DISCOVERY_CONNECT_TIMEOUT),
      parseTimeout(cfg,
                   CLUSTER_DISCOVERY_READ_TIMEOUT,
                   SPARK_CLUSTER_DISCOVERY_READ_TIMEOUT),
      parseTimeout(cfg, CLUSTER_DISCOVERY_DELAY, SPARK_CLUSTER_DISCOVERY_DELAY)
    )
  }

  def parseHttpDiscoveryConfig(
      cfg: SparkConf): Option[ClusterHttpDiscoveryConfig] = {
    val url = cfg
      .getOption(CLUSTER_DISCOVERY_HTTP_URL)
      .orElse(cfg.getOption(SPARK_CLUSTER_DISCOVERY_HTTP_URL))
    if (url.isDefined) {
      Some(ClusterHttpDiscoveryConfig(url.get))
    } else {
      None
    }
  }

  def parseBinaryDiscoveryConfig(
      cfg: SparkConf): Option[ClusterBinaryDiscoveryConfig] = {
    val entryFunction = cfg
      .getOption(CLUSTER_DISCOVERY_BINARY_ENTRY_FUNCTION)
      .orElse(cfg.getOption(SPARK_CLUSTER_DISCOVERY_BINARY_ENTRY_FUNCTION))

    val host = cfg
      .getOption(CLUSTER_DISCOVERY_BINARY_HOST)
      .orElse(cfg.getOption(SPARK_CLUSTER_DISCOVERY_BINARY_HOST))

    if (entryFunction.isDefined && host.isDefined) {
      Some(
        ClusterBinaryDiscoveryConfig(entryFunction.get,
                                     new TarantoolServerAddress(host.get)))
    } else {
      None
    }
  }

  def createReadOptions(space: String, cfg: SparkConf): ReadOptions = {
    val useClusterDiscovery = cfg
      .getOption(USE_CLUSTER_DISCOVERY)
      .orElse(cfg.getOption(SPARK_USE_CLUSTER_DISCOVERY))
    val clusterDiscoveryConfig =
      if (useClusterDiscovery.isDefined && (useClusterDiscovery.get == "true" || useClusterDiscovery.get == "1")) {
        parseClusterDiscoveryConfig(cfg)
      } else {
        None
      }

    val useProxyClient = cfg
      .getOption(USE_PROXY_CLIENT)
      .orElse(cfg.getOption(SPARK_USE_PROXY_CLIENT))

    ReadOptions(
      space = space,
      hosts = parseHosts(cfg),
      credentials = parseCredentials(cfg),
      timeouts = parseTimeouts(cfg),
      useProxyClient = useProxyClient.isDefined && (useProxyClient.get == "true" || useProxyClient.get == "1"),
      clusterDiscoveryConfig = clusterDiscoveryConfig
    )
  }

  def createReadOptions(space: String): ReadOptions = ReadOptions(
    space = space,
    hosts = Seq(new TarantoolServerAddress(TarantoolDefaults.DEFAULT_HOST)),
    useProxyClient = false
  )
}
