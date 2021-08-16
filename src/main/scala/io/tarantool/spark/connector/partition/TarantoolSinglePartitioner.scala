package io.tarantool.spark.connector.partition

import io.tarantool.driver.TarantoolServerAddress
import io.tarantool.driver.api.conditions.Conditions

/**
  * Creates a single partition for the whole space.
  */
class TarantoolSinglePartitioner extends TarantoolPartitioner {

  override def partitions(
    nodes: Seq[TarantoolServerAddress],
    conditions: Conditions
  ): Array[TarantoolPartition] =
    Array(TarantoolPartition(0, nodes, conditions))

}

case object TarantoolSinglePartitioner extends TarantoolSinglePartitioner
