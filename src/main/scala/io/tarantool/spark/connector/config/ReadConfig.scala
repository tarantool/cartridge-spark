package io.tarantool.spark.connector.config

import io.tarantool.spark.connector.partition.{TarantoolPartitioner, TarantoolSinglePartitioner}

case class ReadConfig(
  partitioner: TarantoolPartitioner = new TarantoolSinglePartitioner(),
  batchSize: Int = 1000
)
