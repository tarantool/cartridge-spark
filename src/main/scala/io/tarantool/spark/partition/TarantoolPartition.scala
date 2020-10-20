package io.tarantool.spark.partition

import io.tarantool.spark.connection.ReadOptions
import org.apache.spark.Partition

case class TarantoolPartition(index: Int, options: ReadOptions) extends Partition {
}
