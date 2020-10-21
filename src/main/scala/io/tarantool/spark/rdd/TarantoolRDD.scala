package io.tarantool.spark.rdd

import io.tarantool.driver.api.TarantoolClient
import io.tarantool.driver.api.conditions.Conditions
import io.tarantool.driver.api.cursor.{TarantoolCursor, TarantoolCursorOptions}
import io.tarantool.driver.api.tuple.TarantoolTuple
import io.tarantool.spark.connection.{ReadOptions, TarantoolConnection}
import io.tarantool.spark.partition.TarantoolPartition
import io.tarantool.spark.rdd.api.java.TarantoolJavaRDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

import collection.JavaConverters.asScalaIteratorConverter
import scala.util.Try

class TarantoolRDD(@transient override val sparkContext: SparkContext,
                                val options: ReadOptions) extends RDD[TarantoolTuple](sparkContext, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[TarantoolTuple] = {
    val partitionReadOptions: ReadOptions = split.asInstanceOf[TarantoolPartition].options
    val client = TarantoolConnection().client(partitionReadOptions)

    context.addTaskCompletionListener((ctx: TaskContext) => {
      logDebug("Task completed closing the Tarantool connection")
      Try(client.close())
    })

    createCursor(client, partitionReadOptions).asScala
  }

  private def createCursor(client: TarantoolClient, readOptions: ReadOptions): TarantoolCursor[TarantoolTuple] = {
    val tarantoolSpace = client.space(readOptions.space)

    val conditions = Conditions.any()

    if (readOptions.toBucketId.isDefined && readOptions.fromBucketId.isDefined) {
      conditions.andEquals("bucket_id", readOptions.fromBucketId.get)
    } else {
      if (readOptions.fromBucketId.isDefined) {
        conditions.andGreaterOrEquals("bucket_id", readOptions.fromBucketId.get)
      }

      if (readOptions.toBucketId.isDefined) {
        conditions.andLessOrEquals("bucket_id", readOptions.toBucketId.get)
      }
    }

    tarantoolSpace.cursor(conditions, new TarantoolCursorOptions(readOptions.batchSize))
  }

  override protected def getPartitions: Array[Partition] =
    options.partitioner.createPartitions(options).asInstanceOf[Array[Partition]]

  override def toJavaRDD(): TarantoolJavaRDD = new TarantoolJavaRDD(this)
}
