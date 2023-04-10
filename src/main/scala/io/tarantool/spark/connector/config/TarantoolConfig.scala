package io.tarantool.spark.connector.config

import io.tarantool.driver.api.TarantoolServerAddress
import io.tarantool.spark.connector.config.ErrorTypes.ErrorType
import org.apache.spark.SparkConf

case class Credentials(username: String, password: String) extends Serializable

case class Timeouts(connect: Option[Int], read: Option[Int], request: Option[Int]) extends Serializable

object ErrorTypes extends Enumeration {
  type ErrorType = Value

  val NONE, NETWORK = Value
}

case class Retries(errorType: ErrorType, retryAttempts: Option[Int], delay: Option[Int]) extends Serializable

case class TarantoolConfig(
  hosts: Seq[TarantoolServerAddress],
  credentials: Option[Credentials],
  timeouts: Timeouts,
  connections: Option[Int],
  retries: Option[Retries]
) extends Serializable

object TarantoolConfig {

  private val SPARK_PREFIX = "spark."
  private val PREFIX = "tarantool."

  private val USERNAME = PREFIX + "username"
  private val PASSWORD = PREFIX + "password"

  private val CONNECT_TIMEOUT = PREFIX + "connectTimeout"
  private val READ_TIMEOUT = PREFIX + "readTimeout"
  private val REQUEST_TIMEOUT = PREFIX + "requestTimeout"

  private val HOSTS = PREFIX + "hosts"
  private val CONNECTIONS = PREFIX + "connections"
  private val RETRIES = PREFIX + "retries."

  private val RETRIES_ERROR_TYPE = RETRIES + "errorType"
  private val RETRIES_ATTEMPTS = RETRIES + "maxAttempts"
  private val RETRIES_DELAY = RETRIES + "delay"

  //options with spark. prefix
  private val SPARK_USERNAME = SPARK_PREFIX + USERNAME
  private val SPARK_PASSWORD = SPARK_PREFIX + PASSWORD

  private val SPARK_CONNECT_TIMEOUT = SPARK_PREFIX + CONNECT_TIMEOUT
  private val SPARK_READ_TIMEOUT = SPARK_PREFIX + READ_TIMEOUT
  private val SPARK_REQUEST_TIMEOUT = SPARK_PREFIX + REQUEST_TIMEOUT

  private val SPARK_HOSTS = SPARK_PREFIX + HOSTS
  private val SPARK_CONNECTIONS = SPARK_PREFIX + CONNECTIONS

  private val SPARK_RETRIES_ERROR_TYPE = SPARK_PREFIX + RETRIES_ERROR_TYPE
  private val SPARK_RETRIES_ATTEMPTS = SPARK_PREFIX + RETRIES_ATTEMPTS
  private val SPARK_RETRIES_DELAY = SPARK_PREFIX + RETRIES_DELAY

  def apply(cfg: SparkConf): TarantoolConfig =
    TarantoolConfig(
      parseHosts(cfg),
      parseCredentials(cfg),
      parseTimeouts(cfg),
      parseIntOption(cfg, CONNECTIONS, SPARK_CONNECTIONS),
      parseRetries(cfg)
    )

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
      .filter(_.nonEmpty)
      .map(a => new TarantoolServerAddress(a))

    if (hosts.isEmpty) {
      hosts = Array(new TarantoolServerAddress(TarantoolDefaults.DEFAULT_HOST))
    }

    hosts.toSeq
  }

  def parseTimeouts(cfg: SparkConf): Timeouts =
    Timeouts(
      parseIntOption(cfg, CONNECT_TIMEOUT, SPARK_CONNECT_TIMEOUT),
      parseIntOption(cfg, READ_TIMEOUT, SPARK_READ_TIMEOUT),
      parseIntOption(cfg, REQUEST_TIMEOUT, SPARK_REQUEST_TIMEOUT)
    )

  def parseRetries(cfg: SparkConf): Option[Retries] = {
    val strErrorType = cfg.getOption(RETRIES_ERROR_TYPE).orElse(cfg.getOption(SPARK_RETRIES_ERROR_TYPE))
    val retryAttempts = parseIntOption(cfg, RETRIES_ATTEMPTS, SPARK_RETRIES_ATTEMPTS)
    val retryDelay = parseIntOption(cfg, RETRIES_DELAY, SPARK_RETRIES_DELAY)

    if (strErrorType.isDefined) {
      val errorType = ErrorTypes.withName(strErrorType.get.toUpperCase)

      if (errorType == ErrorTypes.NETWORK) {
        if (retryAttempts.isEmpty) {
          throw new IllegalArgumentException("Number of retry attempts must be specified")
        }

        if (retryDelay.isEmpty) {
          throw new IllegalArgumentException("Delay between retry attempts must be specified")
        }
      }
      Some(
        Retries(
          errorType,
          retryAttempts,
          retryDelay
        )
      )
    } else {
      None
    }
  }

  def parseIntOption(cfg: SparkConf, name: String, nameWithSparkPrefix: String): Option[Int] =
    cfg.getOption(name).orElse(cfg.getOption(nameWithSparkPrefix)).map(_.toInt)
}
