package io.tarantool.spark.connector.config

import io.tarantool.driver.TarantoolServerAddress
import org.apache.spark.SparkConf

case class Credentials(username: String, password: String)

case class Timeouts(connect: Option[Int], read: Option[Int], request: Option[Int])

case class TarantoolConfig(
  hosts: Seq[TarantoolServerAddress],
  credentials: Option[Credentials],
  timeouts: Timeouts
)

object TarantoolConfig {

  private val SPARK_PREFIX = "spark."
  private val PREFIX = "tarantool."

  private val USERNAME = PREFIX + "username"
  private val PASSWORD = PREFIX + "password"

  private val CONNECT_TIMEOUT = PREFIX + "connectTimeout"
  private val READ_TIMEOUT = PREFIX + "readTimeout"
  private val REQUEST_TIMEOUT = PREFIX + "requestTimeout"

  private val HOSTS = PREFIX + "hosts"

  private val CURSOR_BATCH_SIZE = PREFIX + "cursorBatchSize"

  //options with spark. prefix
  private val SPARK_USERNAME = SPARK_PREFIX + USERNAME
  private val SPARK_PASSWORD = SPARK_PREFIX + PASSWORD

  private val SPARK_CONNECT_TIMEOUT = SPARK_PREFIX + CONNECT_TIMEOUT
  private val SPARK_READ_TIMEOUT = SPARK_PREFIX + READ_TIMEOUT
  private val SPARK_REQUEST_TIMEOUT = SPARK_PREFIX + REQUEST_TIMEOUT

  private val SPARK_HOSTS = SPARK_PREFIX + HOSTS

  private val SPARK_CURSOR_BATCH_SIZE = SPARK_PREFIX + CURSOR_BATCH_SIZE

  def parseBatchSize(cfg: SparkConf): Option[Int] =
    cfg.getOption(CURSOR_BATCH_SIZE).orElse(cfg.getOption(SPARK_CURSOR_BATCH_SIZE)).map(_.toInt)

  def apply(cfg: SparkConf): TarantoolConfig =
    TarantoolConfig(parseHosts(cfg), parseCredentials(cfg), parseTimeouts(cfg))

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

    hosts
  }

  def parseTimeouts(cfg: SparkConf): Timeouts =
    Timeouts(
      parseTimeout(cfg, CONNECT_TIMEOUT, SPARK_CONNECT_TIMEOUT),
      parseTimeout(cfg, READ_TIMEOUT, SPARK_READ_TIMEOUT),
      parseTimeout(cfg, REQUEST_TIMEOUT, SPARK_REQUEST_TIMEOUT)
    )

  def parseTimeout(cfg: SparkConf, name: String, nameWithSparkPrefix: String): Option[Int] =
    cfg.getOption(name).orElse(cfg.getOption(nameWithSparkPrefix)).map(_.toInt)
}
