package io.tarantool.spark.connection

import io.tarantool.driver.TarantoolServerAddress
import io.tarantool.spark.partition.{TarantoolPartitioner, TarantoolPartitionerSinglePartition}

case class ReadOptions(space: String,

                       partitioner: TarantoolPartitioner = new TarantoolPartitionerSinglePartition(),
                       hosts: Seq[TarantoolServerAddress],
                       credential: Option[Credential] = None,
                       timeouts: Timeouts = Timeouts(None, None, None),
                       clusterConfig: Option[TarantoolClusterConfig] = None) extends TarantoolConfig {

  def copy(space: String, readOptions: ReadOptions): ReadOptions = {
    ReadOptions(space = space,
      partitioner = readOptions.partitioner,
      hosts = readOptions.hosts,
      credential = readOptions.credential,
      timeouts = readOptions.timeouts,
      clusterConfig = readOptions.clusterConfig
    )
  }
}
