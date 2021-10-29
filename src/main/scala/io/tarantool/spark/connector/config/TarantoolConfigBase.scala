package io.tarantool.spark.connector.config

import org.apache.spark.SparkConf

/**
  * @author Alexey Kuzin
  */
case class TarantoolConfigBase() {

  // TODO: create a class for config parameters
  private val SPARK_PREFIX = "spark."
  private val PREFIX = "tarantool."

  protected def getFromSparkConf(sparkConf: SparkConf, option: String): Option[String] =
    sparkConf
      .getOption(PREFIX + option)
      .orElse(sparkConf.getOption(SPARK_PREFIX + option))

  protected def getFromSparkConfOrOptions(
    sparkConf: SparkConf,
    options: Option[Map[String, String]],
    option: String
  ): Option[String] =
    options match {
      case Some(options) =>
        options
          .get(PREFIX + option)
          .orElse(options.get(SPARK_PREFIX + option))
          .orElse(getFromSparkConf(sparkConf, option))
      case None => getFromSparkConf(sparkConf, option)
    }
}
