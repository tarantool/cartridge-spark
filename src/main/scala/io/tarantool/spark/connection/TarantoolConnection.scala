package io.tarantool.spark.connection

import java.io.{Closeable, Serializable}

import io.tarantool.driver.api.TarantoolClient
import io.tarantool.driver.auth.{
  SimpleTarantoolCredentials,
  TarantoolCredentials
}
import io.tarantool.driver.cluster.{
  BinaryClusterDiscoveryEndpoint,
  BinaryDiscoveryClusterAddressProvider,
  HTTPClusterDiscoveryEndpoint,
  HTTPDiscoveryClusterAddressProvider,
  TarantoolClusterDiscoveryConfig,
  TarantoolClusterDiscoveryEndpoint
}
import io.tarantool.driver.core.TarantoolConnectionSelectionStrategies.ParallelRoundRobinStrategyFactory
import io.tarantool.driver.{
  ClusterTarantoolClient,
  ProxyTarantoolClient,
  TarantoolClientConfig,
  TarantoolClusterAddressProvider
}
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

        val credentials = if (cnf.credentials.isDefined) {
          new SimpleTarantoolCredentials(cnf.credentials.get.username,
                                         cnf.credentials.get.password)
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
        var addressProvider: Option[TarantoolClusterAddressProvider] = None

        if (cnf.clusterDiscoveryConfig.isDefined) {
          addressProvider =
            if (cnf.clusterDiscoveryConfig.get.provider == TarantoolDefaults.DISCOVERY_PROVIDER_HTTP) {
              Some(getHttpProvider(cnf.clusterDiscoveryConfig.get))
            } else {
              Some(
                getBinaryProvider(cnf.clusterDiscoveryConfig.get,
                                  _tarantoolConfig.get.getCredentials))
            }
        }

        if (addressProvider.isDefined) {
          _tarantoolClient = Option(
            new ClusterTarantoolClient(
              _tarantoolConfig.get,
              addressProvider.get,
              ParallelRoundRobinStrategyFactory.INSTANCE))
        } else {
          _tarantoolClient = Option(
            new ClusterTarantoolClient(
              _tarantoolConfig.get,
              new ClusterAddressProvider(cnf.hosts),
              ParallelRoundRobinStrategyFactory.INSTANCE))
        }

        if (cnf.useProxyClient) {
          _tarantoolClient =
            Option(new ProxyTarantoolClient(_tarantoolClient.get))
        }

        logInfo("Created ClusterTarantoolClient, hosts = " + cnf.hosts)
      }

      _tarantoolClient.get
    }
  }

  private def getBinaryProvider(discoveryConfig: ClusterDiscoveryConfig,
                                credentials: TarantoolCredentials) = {
    val binaryDiscoveryConfig: ClusterBinaryDiscoveryConfig =
      discoveryConfig.binaryDiscoveryConfig.get

    val endpoint = new BinaryClusterDiscoveryEndpoint.Builder()
      .withCredentials(credentials)
      .withEntryFunction(binaryDiscoveryConfig.entryFunction)
      .withServerAddress(binaryDiscoveryConfig.address)
      .build

    new BinaryDiscoveryClusterAddressProvider(
      getTarantoolClusterDiscoveryConfig(endpoint, discoveryConfig.timeouts))
  }

  private def getHttpProvider(discoveryConfig: ClusterDiscoveryConfig) = {
    val httpDiscoveryConfig: ClusterHttpDiscoveryConfig =
      discoveryConfig.httpDiscoveryConfig.get

    val endpoint = new HTTPClusterDiscoveryEndpoint.Builder()
      .withURI(httpDiscoveryConfig.url)
      .build

    new HTTPDiscoveryClusterAddressProvider(
      getTarantoolClusterDiscoveryConfig(endpoint, discoveryConfig.timeouts))
  }

  private def getTarantoolClusterDiscoveryConfig(
      endpoint: TarantoolClusterDiscoveryEndpoint,
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
