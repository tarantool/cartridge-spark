package io.tarantool.spark.connector.connection

import io.tarantool.driver.api.tuple.TarantoolTuple
import io.tarantool.driver.api.{
  TarantoolClient,
  TarantoolClientConfig,
  TarantoolResult,
  TarantoolServerAddress
}
import io.tarantool.driver.auth.SimpleTarantoolCredentials
import io.tarantool.driver.core.{ClusterTarantoolTupleClient, ProxyTarantoolTupleClient}
import io.tarantool.driver.protocol.Packable
import io.tarantool.spark.connector.Logging
import io.tarantool.spark.connector.config.{StaticClusterAddressProvider, TarantoolConfig}

import java.io.{Closeable, Serializable}
import java.util
import scala.reflect.ClassTag

/**
  * TarantoolConnection companion object
  */
object TarantoolConnection {

  @transient @volatile private var defaultConnection
    : Option[TarantoolConnection[TarantoolTuple, TarantoolResult[TarantoolTuple]]] = None

  def apply(
    tarantoolConfig: TarantoolConfig
  ): TarantoolConnection[TarantoolTuple, TarantoolResult[TarantoolTuple]] = {
    if (defaultConnection.isEmpty) {
      defaultConnection.synchronized {
        if (defaultConnection.isEmpty) {
          defaultConnection = Option(new TarantoolConnection(tarantoolConfig, defaultClient))
        }
      }
    }
    defaultConnection.get
  }

  private def defaultClient(
    clientConfig: TarantoolClientConfig,
    hosts: Seq[TarantoolServerAddress]
  ): TarantoolClient[TarantoolTuple, TarantoolResult[TarantoolTuple]] =
    new ProxyTarantoolTupleClient(
      new ClusterTarantoolTupleClient(clientConfig, new StaticClusterAddressProvider(hosts))
    )

  def apply[T <: Packable, R <: util.Collection[T]](
    tarantoolConfig: TarantoolConfig,
    configureClient: (TarantoolClientConfig, Seq[TarantoolServerAddress]) => TarantoolClient[T, R]
  )(
    implicit ctt: ClassTag[T],
    ctr: ClassTag[R]
  ): TarantoolConnection[T, R] =
    new TarantoolConnection(tarantoolConfig, configureClient)
}

/**
  * Provides connection to Tarantool server via the Java driver
  */
class TarantoolConnection[T <: Packable, R <: util.Collection[T]](
  tarantoolConfig: TarantoolConfig,
  configureClient: (TarantoolClientConfig, Seq[TarantoolServerAddress]) => TarantoolClient[T, R]
) extends Serializable
    with Closeable
    with Logging {

  @transient @volatile private var _tarantoolConfig: Option[TarantoolClientConfig] = None
  @transient @volatile private var _tarantoolClient: Option[TarantoolClient[T, R]] = None

  def client(): TarantoolClient[T, R] = {
    if (_tarantoolConfig.isEmpty) {
      _tarantoolConfig.synchronized {
        if (_tarantoolConfig.isEmpty) {
          _tarantoolConfig = Option(configBuilder(tarantoolConfig).build())
        }
      }
    }

    if (_tarantoolClient.isEmpty) {
      _tarantoolClient.synchronized {
        if (_tarantoolClient.isEmpty) {
          _tarantoolClient = Option(configureClient(_tarantoolConfig.get, tarantoolConfig.hosts))
          logInfo("Created TarantoolClient, hosts = " + tarantoolConfig.hosts)
        }
      }
    }

    _tarantoolClient.get
  }

  private def configBuilder(cnf: TarantoolConfig) = {
    val builder = new TarantoolClientConfig.Builder()

    val credentials = if (cnf.credentials.isDefined) {
      new SimpleTarantoolCredentials(cnf.credentials.get.username, cnf.credentials.get.password)
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

    builder
  }

  override def close(): Unit =
    if (_tarantoolClient.isDefined) {
      _tarantoolClient.synchronized {
        if (_tarantoolClient.isDefined) {
          logInfo("Closing TarantoolClient")
          _tarantoolClient.get.close()
          _tarantoolClient = None
          _tarantoolConfig = None
        }
      }
    }
}
