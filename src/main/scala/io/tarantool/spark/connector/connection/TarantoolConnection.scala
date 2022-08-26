package io.tarantool.spark.connector.connection

import io.tarantool.driver.api.tuple.TarantoolTuple
import io.tarantool.driver.api.{
  TarantoolClient,
  TarantoolClientConfig,
  TarantoolResult,
  TarantoolServerAddress
}
import io.tarantool.driver.auth.SimpleTarantoolCredentials
import io.tarantool.driver.api.TarantoolClientFactory
import io.tarantool.driver.core.{ClusterTarantoolTupleClient, ProxyTarantoolTupleClient}
import io.tarantool.driver.protocol.Packable
import io.tarantool.spark.connector.Logging
import io.tarantool.spark.connector.config.{StaticClusterAddressProvider, TarantoolConfig}

import java.io.{Closeable, Serializable}
import java.util
import scala.collection.JavaConverters
import scala.reflect.ClassTag

/**
  * TarantoolConnection companion object
  */
object TarantoolConnection {

  def apply(): TarantoolConnection[TarantoolTuple, TarantoolResult[TarantoolTuple]] =
    TarantoolConnection(defaultClient)

  private def defaultClient(
    clientConfig: TarantoolClientConfig,
    hosts: Seq[TarantoolServerAddress]
  ): TarantoolClient[TarantoolTuple, TarantoolResult[TarantoolTuple]] =
    TarantoolClientFactory
      .createClient()
      .withAddresses(JavaConverters.seqAsJavaListConverter(hosts).asJava)
      .withTarantoolClientConfig(clientConfig)
      .withProxyMethodMapping()
      .build()

  def apply[T <: Packable, R <: util.Collection[T]](
    configureClient: (TarantoolClientConfig, Seq[TarantoolServerAddress]) => TarantoolClient[T, R]
  )(
    implicit ctt: ClassTag[T],
    ctr: ClassTag[R]
  ): TarantoolConnection[T, R] =
    new TarantoolConnection(configureClient)

  @transient private lazy val _tarantoolClientPool: scala.collection.mutable.Map[
    Seq[TarantoolServerAddress],
    TarantoolClient[_ <: Packable, _ <: util.Collection[_]]
  ] = scala.collection.concurrent.TrieMap()
}

/**
  * Provides connection to Tarantool server via the Java driver
  */
class TarantoolConnection[T <: Packable, R <: util.Collection[T]](
  configureClient: (TarantoolClientConfig, Seq[TarantoolServerAddress]) => TarantoolClient[T, R]
) extends Serializable
    with Closeable
    with Logging {

  def client(cnf: TarantoolConfig): TarantoolClient[T, R] =
    return TarantoolConnection._tarantoolClientPool
      .getOrElseUpdate(
        cnf.hosts,
        createClient(configBuilder(cnf).build(), cnf.hosts)
      )
      .asInstanceOf[TarantoolClient[T, R]]

  private def createClient(
    tarantoolConfig: TarantoolClientConfig,
    hosts: Seq[TarantoolServerAddress]
  ) = {
    val tarantoolClient = configureClient(tarantoolConfig, hosts)
    logInfo("Created TarantoolClient, hosts = " + hosts)

    tarantoolClient
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

    if (cnf.connections.isDefined) {
      builder.withConnections(cnf.connections.get)
    }

    builder
  }

  override def close(): Unit =
    TarantoolConnection._tarantoolClientPool.foreach {
      case (hosts, client) =>
        logInfo("Closing TarantoolClient for " + hosts)
        try {
          if (client != null) {
            client.close()
          }
        } finally {
          TarantoolConnection._tarantoolClientPool.remove(hosts)
        }
    }
}
