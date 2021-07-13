package io.tarantool.spark.connector.partition

import io.tarantool.driver.TarantoolServerAddress
import io.tarantool.driver.api.conditions.Conditions

/**
  * The TarantoolPartitioner provides the partitions of the whole space (it may lie on a single instance,
  * in a cluster or be sharded in a cluster)
  */
trait TarantoolPartitioner extends Serializable {

  /**
    * Calculate the partitions
    *
    * @param nodes       Tarantool nodes for connecting to
    * @param conditions  query conditions for selecting data
    */
  def partitions(
    nodes: Seq[TarantoolServerAddress],
    conditions: Conditions
  ): Array[TarantoolPartition]
}
