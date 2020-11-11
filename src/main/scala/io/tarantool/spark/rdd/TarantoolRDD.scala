package io.tarantool.spark.rdd

import io.tarantool.driver.api.TarantoolClient
import io.tarantool.driver.api.conditions.Conditions
import io.tarantool.spark.Logging
import io.tarantool.spark.connection.{ReadOptions, TarantoolConnection}
import io.tarantool.spark.partition.TarantoolPartition
import io.tarantool.spark.rdd.api.java.TarantoolJavaRDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

import collection.JavaConverters.asScalaIteratorConverter
import scala.reflect.ClassTag
import scala.util.Try

class TarantoolRDD[T: ClassTag](
    @transient override val sparkContext: SparkContext,
    val options: ReadOptions)
    extends RDD[T](sparkContext, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val client = TarantoolConnection().client(
      split.asInstanceOf[TarantoolPartition].options)

    context.addTaskCompletionListener((ctx: TaskContext) => {
      logDebug("Task completed closing the Tarantool connection")
      Try(client.close())
    })

    createSelect(client, options).asScala
  }

  private def createSelect(client: TarantoolClient, readOptions: ReadOptions)(
      implicit clazz: ClassTag[T]): java.util.Iterator[T] = {
    val tarantoolSpace = client.space(readOptions.space)
    val query = Conditions.any()
    tarantoolSpace
      .select(query, clazz.runtimeClass.asInstanceOf[Class[T]])
      .get()
      .iterator()
  }

  override protected def getPartitions: Array[Partition] =
    options.partitioner.createPartitions(options).asInstanceOf[Array[Partition]]

  override def toJavaRDD(): TarantoolJavaRDD[T] = new TarantoolJavaRDD(this)
}
