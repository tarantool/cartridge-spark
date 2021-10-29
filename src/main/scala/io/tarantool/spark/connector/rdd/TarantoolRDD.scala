package io.tarantool.spark.connector.rdd

import io.tarantool.driver.api.conditions.Conditions
import io.tarantool.driver.api.tuple.{DefaultTarantoolTupleFactory, TarantoolTuple}
import io.tarantool.driver.api.{TarantoolClient, TarantoolResult}
import io.tarantool.driver.mappers.{DefaultMessagePackMapperFactory, MessagePackMapper}
import io.tarantool.spark.connector.config.{ReadConfig, TarantoolConfig}
import io.tarantool.spark.connector.connection.TarantoolConnection
import io.tarantool.spark.connector.partition.TarantoolPartition
import io.tarantool.spark.connector.rdd.converter.{FunctionBasedTupleConverter, TupleConverter}
import io.tarantool.spark.connector.util.TarantoolCursorIterator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.tarantool.MapFunctions.rowToTuple
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

/**
  * Tarantool RDD implementation
  *
  * @param sc spark context
  * @param space Tarantool space name
  * @param conditions tuple filtering conditions
  * @param tupleConverter converter from {@link TarantoolTuple} to type `R`
  * @param readConfig read request configuration
  * @param ct class type tag
  * @tparam R target POJO type
  */
class TarantoolRDD[R] private[spark] (
  @transient val sc: SparkContext,
  val space: String,
  val conditions: Conditions,
  val tupleConverter: TupleConverter[R],
  val readConfig: ReadConfig
)(
  implicit val ct: ClassTag[R],
  implicit val messagePackMapper: MessagePackMapper =
    DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper()
) extends RDD[R](sc, Seq.empty) {

  private val globalConfig = TarantoolConfig(sparkContext.getConf)

  @transient private lazy val tupleFactory = new DefaultTarantoolTupleFactory(
    messagePackMapper
  )

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
      .map(tupleConverter.convert)
  }

  override protected def getPartitions: Array[Partition] =
    readConfig.partitioner.partitions(globalConfig.hosts, conditions).asInstanceOf[Array[Partition]]

  def insert(data: DataFrame, overwrite: Boolean): Unit =
    data.foreachPartition((partition: Iterator[Row]) =>
      if (partition.nonEmpty) {
        val connection = TarantoolConnection()
        val client = connection.client(globalConfig)

        partition.foreach { row =>
          if (overwrite) {
            //TODO use batches when implemented in driver
            //TODO insert bucket ID on the driver side automatically
            client.space(space).replace(rowToTuple(tupleFactory, row))
          } else {
            client.space(space).insert(rowToTuple(tupleFactory, row))
          }
        }

        client.close()
      }
    )
}

object TarantoolRDD {

  def apply[R](
    sc: SparkContext,
    readConfig: ReadConfig,
    converter: TupleConverter[R]
  )(
    implicit
    ct: ClassTag[R]
  ): TarantoolRDD[R] =
    new TarantoolRDD[R](sc, readConfig.spaceName, readConfig.conditions, converter, readConfig)

  def apply(
    sc: SparkContext,
    readConfig: ReadConfig
  )(
    implicit
    ct: ClassTag[TarantoolTuple]
  ): TarantoolRDD[TarantoolTuple] =
    new TarantoolRDD[TarantoolTuple](
      sc,
      readConfig.spaceName,
      readConfig.conditions,
      FunctionBasedTupleConverter(),
      readConfig
    )
}
