package io.tarantool.spark.partition

import io.tarantool.spark.connection.ReadOptions

trait TarantoolPartitioner extends Serializable {

  def createPartitions(options: ReadOptions): Array[TarantoolPartition]

  def createPartition(index: Int,  options: ReadOptions): TarantoolPartition = {
    TarantoolPartition(index, options)
  }
}
