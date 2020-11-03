package io.tarantool.spark.connection

import io.tarantool.driver.TarantoolServerAddress
import io.tarantool.spark.partition.{
  TarantoolPartitioner,
  TarantoolPartitionerSinglePartition
}

case class ReadOptions(space: String,
                       partitioner: TarantoolPartitioner =
                         new TarantoolPartitionerSinglePartition(),
                       hosts: Seq[TarantoolServerAddress],
                       credentials: Option[Credentials] = None,
                       timeouts: Timeouts = Timeouts(None, None, None),
                       useProxyClient: Boolean,
                       clusterDiscoveryConfig: Option[ClusterDiscoveryConfig] =
                         None)
    extends TarantoolConfig {

  def copy(space: String, readOptions: ReadOptions): ReadOptions = {
    ReadOptions(
      space = space,
      partitioner = readOptions.partitioner,
      hosts = readOptions.hosts,
      credentials = readOptions.credentials,
      timeouts = readOptions.timeouts,
      useProxyClient = readOptions.useProxyClient,
      clusterDiscoveryConfig = readOptions.clusterDiscoveryConfig
    )
  }
}
