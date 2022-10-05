package io.tarantool.spark.connector.config

import org.apache.spark.SparkConf

case class WriteConfig(
  spaceName: String,
  batchSize: Int = WriteConfig.DEFAULT_BATCH_SIZE
) {}

object WriteConfig extends TarantoolConfigBase {

  private val SPACE_NAME = "space"
  private val BATCH_SIZE = "batchSize"
  private val DEFAULT_BATCH_SIZE = 1000

  def apply(sparkConf: SparkConf, options: Option[Map[String, String]]): WriteConfig =
    new WriteConfig(
      spaceName = getFromSparkConfOrOptions(sparkConf, options, SPACE_NAME) match {
        case None       => throw new IllegalArgumentException("space name is not specified in parameters")
        case Some(name) => name
      },
      batchSize = getFromSparkConfOrOptions(sparkConf, options, BATCH_SIZE)
        .map(_.toInt)
        .getOrElse(DEFAULT_BATCH_SIZE)
    )

  def apply(spaceName: String): WriteConfig =
    new WriteConfig(spaceName)
}
