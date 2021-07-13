package io.tarantool.spark.connector.partition

import io.tarantool.driver.TarantoolServerAddress
import io.tarantool.driver.api.conditions.Conditions
import org.apache.spark.Partition

/**
  * An identifier for a partition in a TarantoolRDD.
  *
  * @param index The partition's index within its parent RDD
  * @param conditions The query conditions for the data within this partition
  */
case class TarantoolPartition(
  index: Int,
  nodes: Seq[TarantoolServerAddress],
  conditions: Conditions
) extends Partition {
  override def hashCode(): Int = super.hashCode()

  override def equals(other: Any): Boolean = other match {
    case p: TarantoolPartition
        if index.equals(p.index) && conditions.equals(p.conditions) && nodes.equals(p.nodes) =>
      true
    case _ => false
  }

  override def toString: String =
    s"TarantoolPartition(index=$index, conditions=${conditions.toString})"
}
