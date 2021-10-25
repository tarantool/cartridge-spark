package io.tarantool.spark.connector.config

import io.tarantool.driver.api.conditions.Conditions
import io.tarantool.spark.connector.partition.{TarantoolPartitioner, TarantoolSinglePartitioner}
import org.apache.spark.SparkConf

case class ReadConfig(
  spaceName: String,
  partitioner: TarantoolPartitioner = new TarantoolSinglePartitioner(),
  conditions: Conditions = Conditions.any(),
  batchSize: Int = 1000
) {

  def withConditions(conditions: Conditions): ReadConfig =
    copy(conditions = conditions)
}

object ReadConfig {

  // TODO: create a class for config parameters
  private val SPARK_PREFIX = "spark."
  private val PREFIX = "tarantool."

  private val SPACE_NAME = "space"
  private val CURSOR_BATCH_SIZE = "cursorBatchSize"

  private def getFromSparkConfOrOptions(
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

  private def getFromSparkConf(sparkConf: SparkConf, option: String): Option[String] =
    sparkConf
      .getOption(PREFIX + option)
      .orElse(sparkConf.getOption(SPARK_PREFIX + option))

  def apply(sparkConf: SparkConf, options: Option[Map[String, String]]): ReadConfig =
    new ReadConfig(
      spaceName = getFromSparkConfOrOptions(sparkConf, options, SPACE_NAME) match {
        case None       => throw new IllegalArgumentException("space name is not specified in parameters")
        case Some(name) => name
      },
      batchSize = getFromSparkConfOrOptions(sparkConf, options, CURSOR_BATCH_SIZE)
        .map(_.toInt)
        .getOrElse(1000)
    )

  def apply(spaceName: String): ReadConfig =
    new ReadConfig(spaceName)
}
