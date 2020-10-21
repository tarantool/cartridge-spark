package io.tarantool.spark.connection

import java.io.{Closeable, Serializable}

import io.tarantool.driver.api.TarantoolClient
import io.tarantool.driver.auth.{SimpleTarantoolCredentials, TarantoolCredentials}
import io.tarantool.driver.cluster.{BinaryClusterDiscoveryEndpoint, BinaryDiscoveryClusterAddressProvider, HTTPClusterDiscoveryEndpoint, HTTPDiscoveryClusterAddressProvider, TarantoolClusterDiscoveryConfig, TarantoolClusterDiscoveryEndpoint}
import io.tarantool.driver.core.TarantoolConnectionSelectionStrategies.ParallelRoundRobinStrategyFactory
import io.tarantool.driver.{ClusterTarantoolClient, ProxyTarantoolClient, StandaloneTarantoolClient, TarantoolClientConfig, TarantoolClusterAddressProvider}
import io.tarantool.spark.Logging

/**
 * The TarantoolConnector companion object
 *
 */
object TarantoolConnection {
  lazy val connection = new TarantoolConnection()

  def apply(): TarantoolConnection = connection
}

/**
 * The TarantoolConnector
 *
 * Connects Spark to Tarantool
 *
 */
class TarantoolConnection extends Serializable with Closeable with Logging {

  @transient var _tarantoolConfig: Option[TarantoolClientConfig] = None
  @transient var _tarantoolClient: Option[TarantoolClient] = None

  def client(cnf: TarantoolConfig): TarantoolClient = {

    this.synchronized {
      logInfo(s"Creating TarantoolClient")

      if (_tarantoolConfig.isEmpty) {
        val builder = new TarantoolClientConfig.Builder()

        val credentials = if (cnf.credential.isDefined) {
          new SimpleTarantoolCredentials(cnf.credential.get.username, cnf.credential.get.password)
        } else {
          new SimpleTarantoolCredentials()
        }
        builder.withCredentials(credentials)

        if (cnf.timeouts.connect.isDefined) {
          builder.withConnectTimeout(cnf.timeouts.connect.get)
        }

        if (cnf.timeouts.read.isDefined) {
          builder.withReadTimeout(cnf.timeouts.read.get)
        }

        if (cnf.timeouts.request.isDefined) {
          builder.withRequestTimeout(cnf.timeouts.request.get)
        }

        _tarantoolConfig = Option(builder.build())
      }

      if (_tarantoolClient.isEmpty) {
        if (cnf.clusterConfig.isDefined) {
          val clusterConfig = cnf.clusterConfig.get

          var addressProvider: Option[TarantoolClusterAddressProvider] = None

          if (clusterConfig.discoveryConfig.isDefined) {
            addressProvider = if (clusterConfig.discoveryConfig.get.provider == TarantoolDefaults.DISCOVERY_PROVIDER_HTTP) {
              Some(getHttpProvider(cnf))
            } else {
              Some(getBinaryProvider(cnf, _tarantoolConfig.get.getCredentials))
            }
          }

          var clusterTarantoolClient: Option[ClusterTarantoolClient] = None
          if (addressProvider.isDefined) {
            clusterTarantoolClient = Option(new ClusterTarantoolClient(
              _tarantoolConfig.get,
              addressProvider.get,
              ParallelRoundRobinStrategyFactory.INSTANCE)
            )
          } else {
            clusterTarantoolClient = Option(new ClusterTarantoolClient(
              _tarantoolConfig.get,
              new ClusterAddressProvider(cnf.hosts),
              ParallelRoundRobinStrategyFactory.INSTANCE)
            )
          }

          _tarantoolClient = Option(new ProxyTarantoolClient(clusterTarantoolClient.get))

          logInfo("Created ClusterTarantoolClient")
        } else {
          _tarantoolClient = Option(new StandaloneTarantoolClient(_tarantoolConfig.get, cnf.hosts.head))
          logInfo("Created StandaloneTarantoolClient")
        }
      }

      _tarantoolClient.get
    }
  }

  private def getBinaryProvider(tarantoolConfig: TarantoolConfig, credentials: TarantoolCredentials) = {
    val clusterConfig = tarantoolConfig.clusterConfig.get
    val discoveryConfig = clusterConfig.discoveryConfig.get
    val binaryDiscoveryConfig: ClusterBinaryDiscoveryConfig = discoveryConfig.binaryDiscoveryConfig.get

    val endpoint = new BinaryClusterDiscoveryEndpoint.Builder()
      .withCredentials(credentials)
      .withEntryFunction(binaryDiscoveryConfig.entryFunction)
      .withServerAddress(binaryDiscoveryConfig.address)
      .build

    new BinaryDiscoveryClusterAddressProvider(getTarantoolClusterDiscoveryConfig(endpoint, discoveryConfig.timeouts))
  }

  private def getHttpProvider(tarantoolConfig: TarantoolConfig) = {
    val clusterConfig = tarantoolConfig.clusterConfig.get
    val discoveryConfig = clusterConfig.discoveryConfig.get
    val httpDiscoveryConfig: ClusterHttpDiscoveryConfig = discoveryConfig.httpDiscoveryConfig.get

    val endpoint = new HTTPClusterDiscoveryEndpoint.Builder()
      .withURI(httpDiscoveryConfig.url)
      .build

    new HTTPDiscoveryClusterAddressProvider(getTarantoolClusterDiscoveryConfig(endpoint, discoveryConfig.timeouts))
  }

  private def getTarantoolClusterDiscoveryConfig(endpoint: TarantoolClusterDiscoveryEndpoint,
                                                 timeouts: ClusterDiscoveryTimeouts): TarantoolClusterDiscoveryConfig = {
    val builder = new TarantoolClusterDiscoveryConfig.Builder()
      .withEndpoint(endpoint)

    if (timeouts.connect.isDefined) {
      builder.withConnectTimeout(timeouts.connect.get)
    }

    if (timeouts.read.isDefined) {
      builder.withReadTimeout(timeouts.read.get)
    }

    if (timeouts.delay.isDefined) {
      builder.withDelay(timeouts.delay.get)
    }

    builder.build
  }

  override def close(): Unit = {
    this.synchronized {
      if (_tarantoolClient.isDefined) {
        logInfo("Closing TarantoolClient")
        _tarantoolClient.get.close()
        _tarantoolClient = None
        _tarantoolConfig = None
      }
    }
  }
}
