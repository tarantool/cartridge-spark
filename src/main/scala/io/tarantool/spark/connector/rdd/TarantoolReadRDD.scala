package io.tarantool.spark.connector.rdd

import io.tarantool.driver.api.conditions.Conditions
import io.tarantool.driver.api.tuple.TarantoolTuple
import io.tarantool.driver.api.{TarantoolClient, TarantoolResult}
import io.tarantool.driver.mappers.MessagePackMapper
import io.tarantool.driver.mappers.factories.DefaultMessagePackMapperFactory
import io.tarantool.spark.connector.config.{ReadConfig, TarantoolConfig}
import io.tarantool.spark.connector.connection.TarantoolConnection
import io.tarantool.spark.connector.partition.TarantoolPartition
import io.tarantool.spark.connector.rdd.converter.{FunctionBasedTupleConverter, TupleConverter}
import io.tarantool.spark.connector.util.TarantoolCursorIterator
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

/**
  * Tarantool RDD implementation for read operations
  *
  * @param sc spark context
  * @param space Tarantool space name
  * @param conditions tuple filtering conditions
  * @param tupleConverter converter from {@link TarantoolTuple} to type `R`
  * @param readConfig read request configuration
  * @param ct class type tag
  * @tparam R target POJO type
  */
class TarantoolReadRDD[R] private[spark] (
  @transient val sc: SparkContext,
  val space: String,
  val conditions: Conditions,
  val tupleConverter: TupleConverter[R],
  val readConfig: ReadConfig
)(
  implicit ct: ClassTag[R],
  implicit val messagePackMapper: MessagePackMapper =
    DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper()
) extends RDD[R](sc, Seq.empty)
    with TarantoolBaseRDD {

  private val globalConfig = TarantoolConfig(sc.getConf)

  override def compute(split: Partition, context: TaskContext): Iterator[R] = {
    val partition = split.asInstanceOf[TarantoolPartition]
    val connection = TarantoolConnection()
    context.addTaskCompletionListener {
      new Function1[TaskContext, Unit] {
        def apply(context: TaskContext): Unit = connection.close()
      }
    }

    val client = connection.client(globalConfig)
    createCursorIterator(client, partition)
  }

  private def createCursorIterator(
    client: TarantoolClient[TarantoolTuple, TarantoolResult[TarantoolTuple]],
    partition: TarantoolPartition
  ): Iterator[R] = {
    val tarantoolSpace = client.space(space)
    // TODO add limit and offset to conditions based on partition information
    TarantoolCursorIterator(tarantoolSpace.cursor(conditions, readConfig.cursorBatchSize))
      .map(tupleConverter.convert)
  }

  override protected def getPartitions: Array[Partition] =
    readConfig.partitioner.partitions(globalConfig.hosts, conditions).asInstanceOf[Array[Partition]]
}

object TarantoolReadRDD {

  def apply[R](
    sc: SparkContext,
    readConfig: ReadConfig,
    converter: TupleConverter[R]
  )(
    implicit
    ct: ClassTag[R]
  ): TarantoolReadRDD[R] =
    new TarantoolReadRDD[R](sc, readConfig.spaceName, readConfig.conditions, converter, readConfig)

  def apply(
    sc: SparkContext,
    readConfig: ReadConfig
  )(
    implicit
    ct: ClassTag[TarantoolTuple]
  ): TarantoolReadRDD[TarantoolTuple] =
    new TarantoolReadRDD[TarantoolTuple](
      sc,
      readConfig.spaceName,
      readConfig.conditions,
      FunctionBasedTupleConverter(),
      readConfig
    )
}
