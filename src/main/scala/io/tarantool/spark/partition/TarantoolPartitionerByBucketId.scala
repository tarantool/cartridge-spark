package io.tarantool.spark.partition

import io.tarantool.driver.exceptions.TarantoolClientException
import io.tarantool.spark.connection.{ReadOptions, TarantoolConnection}

class TarantoolPartitionerByBucketId(bucketCountFunction: String,
                                     bucketForPartition: Int = 100,
                                     batchSize: Option[Int] = None) extends TarantoolPartitioner {

  override def createPartitions(options: ReadOptions): Array[TarantoolPartition] = {
    val bucketsCount = getBucketsCount(options)
    val partitionBatchSize: Int = batchSize.getOrElse(options.batchSize)

    if (bucketsCount < bucketForPartition) {
      return Array(createPartition(0, options.copy(fromBucketId = None, toBucketId = None, batchSize = partitionBatchSize)))
    }

    var partitionNumber: Int = 0
    val partitions = scala.collection.mutable.ArrayBuffer.empty[TarantoolPartition]
    do {
      val from = Some(partitionNumber * bucketForPartition + 1)
      val to = Some(Math.min((partitionNumber + 1) * bucketForPartition, bucketsCount))

      partitions += createPartition(partitionNumber, options.copy(fromBucketId = from, toBucketId = to, batchSize = partitionBatchSize))
      partitionNumber += 1
    } while (partitionNumber * bucketForPartition < bucketsCount)

    partitions.toArray
  }

  def getBucketsCount(options: ReadOptions): Int = {
    val tarantoolConnection = TarantoolConnection()
    val tarantoolClient = tarantoolConnection.client(options)

    val res = tarantoolClient.call(bucketCountFunction).get()
    var bucketsCount: Int = -1

    if (res.size() == 1) {
      bucketsCount = res.get(0) match {
        case n: java.lang.Integer => n
        case n: java.lang.Long => n.toInt
        case _ => -1
      }
    }

    if (res.size() != 1 || bucketsCount <= 0) {
      throw new TarantoolClientException("Invalid bucket function result")
    }

    bucketsCount
  }

}
