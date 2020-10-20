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
    val client = TarantoolConnection().client(split.asInstanceOf[TarantoolPartition].options)

    context.addTaskCompletionListener((ctx: TaskContext) => {
      logDebug("Task completed closing the Tarantool connection")
      Try(client.close())
    })

    createCursor(client, options).asScala
  }

  private def createCursor(client: TarantoolClient, readOptions: ReadOptions): TarantoolCursor[TarantoolTuple] = {
    val tarantoolSpace = client.space(readOptions.space)

    tarantoolSpace.cursor(Conditions.any(), new TarantoolCursorOptions())
  }

  override protected def getPartitions: Array[Partition] =
    options.partitioner.createPartitions(options).asInstanceOf[Array[Partition]]

  override def toJavaRDD(): TarantoolJavaRDD = new TarantoolJavaRDD(this)
}
