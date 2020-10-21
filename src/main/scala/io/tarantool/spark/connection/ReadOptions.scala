package io.tarantool.spark.connection

import io.tarantool.driver.TarantoolServerAddress
import io.tarantool.spark.connection.TarantoolDefaults.DEFAULT_BATCH_SIZE
import io.tarantool.spark.partition.{TarantoolPartitioner, TarantoolPartitionerSinglePartition}

case class ReadOptions(space: String,
                       partitioner: TarantoolPartitioner = new TarantoolPartitionerSinglePartition(),
                       hosts: Seq[TarantoolServerAddress],
                       batchSize: Int = DEFAULT_BATCH_SIZE,
                       credential: Option[Credential] = None,
                       timeouts: Timeouts = Timeouts(None, None, None),
                       clusterConfig: Option[TarantoolClusterConfig] = None,
                       fromBucketId: Option[Int] = None,
                       toBucketId: Option[Int] = None) extends TarantoolConfig {

  def copy(space: String = space,
           partitioner: TarantoolPartitioner = partitioner,
           hosts: Seq[TarantoolServerAddress] = hosts,
           batchSize: Int = batchSize,
           credential: Option[Credential] = credential,
           timeouts: Timeouts = timeouts,
           clusterConfig: Option[TarantoolClusterConfig] = clusterConfig,
           fromBucketId: Option[Int] = fromBucketId,
           toBucketId: Option[Int] = toBucketId): ReadOptions = {
    ReadOptions(space, partitioner, hosts, batchSize, credential, timeouts, clusterConfig, fromBucketId, toBucketId)
  }
}
