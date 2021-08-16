package io.tarantool.spark.connector.config

import io.tarantool.spark.connector.partition.{TarantoolPartitioner, TarantoolSinglePartitioner}
import org.apache.spark.SparkConf

case class ReadConfig(
  partitioner: TarantoolPartitioner = new TarantoolSinglePartitioner(),
  batchSize: Int = 1000
)

object ReadConfig {

  // TODO: create a class for config parameters
  private val SPARK_PREFIX = "spark."
  private val PREFIX = "tarantool."

  private val CURSOR_BATCH_SIZE = PREFIX + "cursorBatchSize"
  private val SPARK_CURSOR_BATCH_SIZE = SPARK_PREFIX + CURSOR_BATCH_SIZE

  def fromSparkConf(sparkConfig: SparkConf): ReadConfig =
    ReadConfig(
      batchSize = sparkConfig
        .getOption(CURSOR_BATCH_SIZE)
        .orElse(sparkConfig.getOption(SPARK_CURSOR_BATCH_SIZE))
        .map(_.toInt)
        .getOrElse(1000)
    )
}
