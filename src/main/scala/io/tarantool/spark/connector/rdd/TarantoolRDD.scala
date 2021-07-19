package io.tarantool.spark.connector.rdd

import io.tarantool.driver.api.conditions.Conditions
import io.tarantool.driver.api.tuple.TarantoolTuple
import io.tarantool.driver.api.{TarantoolClient, TarantoolResult}
import io.tarantool.spark.connector.config.{ReadConfig, TarantoolConfig}
import io.tarantool.spark.connector.connection.TarantoolConnection
import io.tarantool.spark.connector.partition.TarantoolPartition
import io.tarantool.spark.connector.rdd.api.java.TarantoolJavaRDD
import io.tarantool.spark.connector.util.TarantoolCursorIterator
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

class TarantoolRDD[R] private[spark] (
  @transient val sc: SparkContext,
  val space: String,
  val conditions: Conditions,
  val readConfig: ReadConfig
)(
  implicit val ct: ClassTag[R],
  implicit val tupleConverter: TarantoolTuple => R
) extends RDD[R](sc, Seq.empty) {

  private val globalConfig = TarantoolConfig(sparkContext.getConf)

  override def compute(split: Partition, context: TaskContext): Iterator[R] = {
    val partition = split.asInstanceOf[TarantoolPartition]
    val connection = TarantoolConnection()
    val client = connection.client(globalConfig)
    val cursorIterator = createCursorIterator(client, partition)

    context.addTaskCompletionListener { context =>
      connection.close()
      context
    }

    cursorIterator
  }

  private def createCursorIterator(
    client: TarantoolClient[TarantoolTuple, TarantoolResult[TarantoolTuple]],
    partition: TarantoolPartition
  ): Iterator[R] = {
    val tarantoolSpace = client.space(space)
    // TODO add limit and offset to conditions based on partition information
    TarantoolCursorIterator(tarantoolSpace.cursor(conditions, readConfig.batchSize))
      .map(tupleConverter)
  }

  override def toJavaRDD(): TarantoolJavaRDD[R] = new TarantoolJavaRDD(this)

  override protected def getPartitions: Array[Partition] =
    readConfig.partitioner.partitions(globalConfig.hosts, conditions).asInstanceOf[Array[Partition]]
}

object TarantoolRDD {

  def apply[R](
    sc: SparkContext,
    space: String,
    conditions: Conditions,
    readConfig: ReadConfig,
    converter: TarantoolTuple => R
  )(
    implicit
    ct: ClassTag[R],
    tupleConverter: TarantoolTuple => R = converter
  ): TarantoolRDD[R] =
    new TarantoolRDD[R](sc, space, conditions, readConfig)
}
