package io.tarantool.spark.connector.config

import org.apache.spark.SparkConf

case class WriteConfig(
  spaceName: String,
  batchSize: Int = 1000
) {}

object WriteConfig extends TarantoolConfigBase {

  private val SPACE_NAME = "space"
  private val CURSOR_BATCH_SIZE = "cursorBatchSize"

  def apply(sparkConf: SparkConf, options: Option[Map[String, String]]): WriteConfig =
    new WriteConfig(
      spaceName = getFromSparkConfOrOptions(sparkConf, options, SPACE_NAME) match {
        case None       => throw new IllegalArgumentException("space name is not specified in parameters")
        case Some(name) => name
      },
      batchSize = getFromSparkConfOrOptions(sparkConf, options, CURSOR_BATCH_SIZE)
        .map(_.toInt)
        .getOrElse(1000)
    )

  def apply(spaceName: String): WriteConfig =
    new WriteConfig(spaceName)
}