package io.tarantool.spark.connector.config

import io.tarantool.driver.api.conditions.Conditions
import io.tarantool.spark.connector.partition.{TarantoolPartitioner, TarantoolSinglePartitioner}
import org.apache.spark.SparkConf

case class ReadConfig(
  spaceName: String,
  cursorBatchSize: Int = ReadConfig.DEFAULT_CURSOR_BATCH_SIZE,
  batchSize: Int = ReadConfig.DEFAULT_BATCH_SIZE,
  partitioner: TarantoolPartitioner = new TarantoolSinglePartitioner(),
  conditions: Conditions = Conditions.any()
) extends Serializable {

  def withConditions(conditions: Conditions): ReadConfig =
    copy(conditions = conditions)
}

object ReadConfig extends TarantoolConfigBase {

  private val SPACE_NAME = "space"
  private val BATCH_SIZE = "batchSize"
  private val CURSOR_BATCH_SIZE = "cursorBatchSize"
  private val DEFAULT_BATCH_SIZE = 1000
  private val DEFAULT_CURSOR_BATCH_SIZE = 1000

  def apply(sparkConf: SparkConf, options: Option[Map[String, String]]): ReadConfig =
    new ReadConfig(
      spaceName = getFromSparkConfOrOptions(sparkConf, options, SPACE_NAME) match {
        case None       => throw new IllegalArgumentException("space name is not specified in parameters")
        case Some(name) => name
      },
      batchSize = getFromSparkConfOrOptions(sparkConf, options, BATCH_SIZE)
        .map(_.toInt)
        .getOrElse(DEFAULT_BATCH_SIZE),
      cursorBatchSize = getFromSparkConfOrOptions(sparkConf, options, CURSOR_BATCH_SIZE)
        .map(_.toInt)
        .getOrElse(DEFAULT_CURSOR_BATCH_SIZE)
    )

  def apply(spaceName: String): ReadConfig =
    new ReadConfig(spaceName)
}
