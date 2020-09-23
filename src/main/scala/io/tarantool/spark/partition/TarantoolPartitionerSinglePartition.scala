package io.tarantool.spark.partition

import io.tarantool.spark.connection.ReadOptions

class TarantoolPartitionerSinglePartition extends TarantoolPartitioner {
  override def createPartitions(options: ReadOptions): Array[TarantoolPartition] =
    Array(createPartition(0, options))
}
