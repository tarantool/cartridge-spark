package io.tarantool.spark.connector.connection

import io.tarantool.driver.api.tuple.TarantoolTuple
import io.tarantool.driver.api.{TarantoolClient, TarantoolClientConfig, TarantoolResult, TarantoolServerAddress}
import io.tarantool.driver.auth.SimpleTarantoolCredentials
import io.tarantool.driver.api.TarantoolClientFactory
import io.tarantool.driver.api.retry.TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicyFactory
import io.tarantool.driver.api.retry.TarantoolRequestRetryPolicies.retryNetworkErrors
import io.tarantool.driver.exceptions.TarantoolInternalException
import io.tarantool.driver.protocol.Packable
import io.tarantool.spark.connector.Logging
import io.tarantool.spark.connector.config.{ErrorTypes, TarantoolConfig}
import io.tarantool.spark.connector.util.ScalaToJavaHelper.{toJavaPredicate, toJavaUnaryOperator}

import java.io.{Closeable, Serializable}
import java.util
import java.util.function.Predicate
import scala.collection.JavaConverters
import scala.reflect.ClassTag

/**
  * TarantoolConnection companion object
  */
object TarantoolConnection {

  def apply(): TarantoolConnection[TarantoolTuple, TarantoolResult[TarantoolTuple]] =
    TarantoolConnection(defaultClient)

  private def isConflictError(e: Throwable): Boolean =
    e.isInstanceOf[TarantoolInternalException] &&
      e.getMessage.indexOf("Transaction has been aborted by conflict") > 0

  private def retryConflictErrors(): Predicate[Throwable] = toJavaPredicate(isConflictError)

  private def defaultClient(
    clientConfig: TarantoolConfig
  ): TarantoolClient[TarantoolTuple, TarantoolResult[TarantoolTuple]] = {
    val tarantoolConfig = configBuilder(clientConfig).build()
    var clientFactory = TarantoolClientFactory
      .createClient()
      .withAddresses(JavaConverters.seqAsJavaListConverter(clientConfig.hosts).asJava)
      .withTarantoolClientConfig(tarantoolConfig)
      .withProxyMethodMapping()

    if (clientConfig.retries.isDefined) {
      val retries = clientConfig.retries.get
      if (retries.errorType != ErrorTypes.NONE) {
        val predicate = retries.errorType match {
          case ErrorTypes.NETWORK  => retryNetworkErrors
          case ErrorTypes.CONFLICT => retryConflictErrors
          case _                   => retryNetworkErrors.and(retryConflictErrors)
        }
        clientFactory = clientFactory.withRetryingByNumberOfAttempts(
          retries.retryAttempts.get,
          predicate,
          toJavaUnaryOperator { policyBuilder: AttemptsBoundRetryPolicyFactory.Builder[Predicate[Throwable]] =>
            policyBuilder.withDelay(retries.delay.get)
          }
        )
      }
    }

    clientFactory.build()
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

  def apply[T <: Packable, R <: util.Collection[T]](
    configureClient: (TarantoolConfig) => TarantoolClient[T, R]
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
  configureClient: (TarantoolConfig) => TarantoolClient[T, R]
) extends Serializable
    with Closeable
    with Logging {

  def client(cnf: TarantoolConfig): TarantoolClient[T, R] =
    TarantoolConnection._tarantoolClientPool
      .getOrElseUpdate(
        cnf.hosts,
        createClient(cnf)
      )
      .asInstanceOf[TarantoolClient[T, R]]

  private def createClient(
    tarantoolConfig: TarantoolConfig
  ) = {
    val tarantoolClient = configureClient(tarantoolConfig)
    logInfo("Created TarantoolClient, hosts = " + tarantoolConfig.hosts)

    tarantoolClient
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
