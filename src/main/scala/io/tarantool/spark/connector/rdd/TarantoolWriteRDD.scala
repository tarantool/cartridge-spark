package io.tarantool.spark.connector.rdd

import io.tarantool.driver.api.conditions.Conditions
import io.tarantool.driver.api.tuple.{DefaultTarantoolTupleFactory, TarantoolTuple}
import io.tarantool.driver.api.TarantoolResult
import io.tarantool.driver.api.space.options.proxy.ProxyInsertManyOptions
import io.tarantool.driver.api.space.options.proxy.ProxyReplaceManyOptions
import io.tarantool.driver.mappers.{DefaultMessagePackMapperFactory, MessagePackMapper}
import io.tarantool.spark.connector.{Logging, TarantoolSparkException}
import io.tarantool.spark.connector.config.{TarantoolConfig, WriteConfig}
import io.tarantool.spark.connector.connection.TarantoolConnection
import io.tarantool.spark.connector.util.ScalaToJavaHelper.{toJavaBiFunction, toJavaFunction}
import org.apache.spark.sql.tarantool.MapFunctions.rowToTuple
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.SparkContext

import java.io.{PrintWriter, StringWriter}
import java.util.Collections
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicLong
import java.util.{List => JList}
import java.util.{LinkedList => JLinkedList}
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConverters
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
  * Tarantool RDD implementation for write operations
  *
  * @param sc spark context
  * @param space Tarantool space name
  * @param writeConfig write request configuration
  * @param ct class type tag
  * @tparam R target POJO type
  */
class TarantoolWriteRDD[R] private[spark] (
  @transient val sc: SparkContext,
  val space: String,
  val writeConfig: WriteConfig
)(
  implicit ct: ClassTag[R],
  implicit val messagePackMapper: MessagePackMapper =
    DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper()
) extends TarantoolBaseRDD
    with Serializable
    with Logging {

  private val globalConfig = TarantoolConfig(sc.getConf)

  def isEmpty(
    connection: TarantoolConnection[TarantoolTuple, TarantoolResult[TarantoolTuple]]
  ): Boolean = {
    val client = connection.client(globalConfig)

    client.space(space).select(Conditions.any().withLimit(1)).get().size() == 0
  }

  def nonEmpty(
    connection: TarantoolConnection[TarantoolTuple, TarantoolResult[TarantoolTuple]]
  ): Boolean = !isEmpty(connection)

  def truncate(
    connection: TarantoolConnection[TarantoolTuple, TarantoolResult[TarantoolTuple]]
  ): Unit = {
    val client = connection.client(globalConfig)

    client.space(space).truncate().get()
  }

  def write(
    connection: TarantoolConnection[TarantoolTuple, TarantoolResult[TarantoolTuple]],
    data: DataFrame,
    overwrite: Boolean
  ): Unit =
    data.foreachPartition((partition: Iterator[Row]) =>
      if (partition.nonEmpty) {
        val client = connection.client(globalConfig)
        val spaceMetadata = client.metadata().getSpaceByName(space).get()
        val tupleFactory = new DefaultTarantoolTupleFactory(messagePackMapper, spaceMetadata)

        val options: Either[ProxyReplaceManyOptions, ProxyInsertManyOptions] = if (overwrite) {
          Left(
            ProxyReplaceManyOptions
              .create()
              .withRollbackOnError(writeConfig.rollbackOnError)
              .withStopOnError(writeConfig.stopOnError)
          )
        } else {
          Right(
            ProxyInsertManyOptions
              .create()
              .withRollbackOnError(writeConfig.rollbackOnError)
              .withStopOnError(writeConfig.stopOnError)
          )
        }
        val operation = options match {
          case Left(options) =>
            (tuples: Iterable[TarantoolTuple]) =>
              client
                .space(space)
                .replaceMany(JavaConverters.seqAsJavaListConverter(tuples.toList).asJava, options)
          case Right(options) =>
            (tuples: Iterable[TarantoolTuple]) =>
              client
                .space(space)
                .insertMany(JavaConverters.seqAsJavaListConverter(tuples.toList).asJava, options)
        }

        val tupleStream: Iterator[TarantoolTuple] =
          partition.map(row => rowToTuple(tupleFactory, row, writeConfig.transformFieldNames))

        if (writeConfig.stopOnError) {
          writeSync(tupleStream, operation)
        } else {
          writeAsync(tupleStream, operation)
        }
      }
    )

  type AsyncTarantoolResult = CompletableFuture[TarantoolResult[TarantoolTuple]]

  private def writeSync(
    tupleStream: Iterator[TarantoolTuple],
    operation: Iterable[TarantoolTuple] => AsyncTarantoolResult
  ): Unit = {
    val rowCount: AtomicLong = new AtomicLong(0)
    val tuples: ListBuffer[TarantoolTuple] = ListBuffer()
    var future: Option[AsyncTarantoolResult] =
      tupleStream
        .foldLeft(Option.empty[AsyncTarantoolResult]) {
          (
            parentFuture: Option[AsyncTarantoolResult],
            tuple: TarantoolTuple
          ) =>
            if (tuples.size >= writeConfig.batchSize) {
              val batch = tuples.clone()
              def nextFuture(): AsyncTarantoolResult = {
                val expectedCount = writeConfig.batchSize
                operation(batch)
                  .thenApply(toJavaFunction { result: TarantoolResult[TarantoolTuple] =>
                    if (result.size != expectedCount) {
                      throw batchUnsuccessfulException(tuples)
                    }
                    rowCount.getAndAdd(expectedCount)
                    result
                  })
              }
              val future = if (parentFuture.isDefined) {
                Some(parentFuture.get.thenCompose(toJavaFunction { previousResult: TarantoolResult[TarantoolTuple] =>
                  nextFuture()
                }))
              } else {
                Some(nextFuture())
              }
              tuples.clear()
              tuples += tuple
              future
            } else {
              tuples += tuple
              parentFuture
            }
        }

    if (tuples.nonEmpty) {
      def nextFuture(): AsyncTarantoolResult = {
        val expectedCount = tuples.size
        operation(tuples)
          .thenApply(toJavaFunction { result: TarantoolResult[TarantoolTuple] =>
            if (result.size != expectedCount) {
              throw batchUnsuccessfulException(tuples)
            }
            rowCount.getAndAdd(expectedCount)
            result
          })
      }
      future = Some(future match {
        case Some(future) =>
          future.thenCompose(toJavaFunction { previousResult: TarantoolResult[TarantoolTuple] => nextFuture() })
        case None => nextFuture()
      })
    }

    future.get
      .handle(toJavaBiFunction { (_: TarantoolResult[TarantoolTuple], exception: Throwable) =>
        if (Option(exception).isDefined) {
          exception match {
            case e: RuntimeException => throw e
            case e: Any              => throw TarantoolSparkException(e)
          }
        } else {
          logInfo(s"Dataset write success, ${rowCount.get()} rows written")
        }
        null
      })
      .join()
  }

  private def writeAsync(
    tupleStream: Iterator[TarantoolTuple],
    operation: Iterable[TarantoolTuple] => AsyncTarantoolResult
  ): Unit = {
    val rowCount: AtomicLong = new AtomicLong(0)
    val failedRowsExceptions: JList[Throwable] =
      Collections.synchronizedList(new JLinkedList[Throwable]());
    val tuples: ListBuffer[TarantoolTuple] = ListBuffer()
    val allFutures: ListBuffer[CompletableFuture[_]] =
      tupleStream
        .foldLeft(ListBuffer[CompletableFuture[_]]()) {
          (futures: ListBuffer[CompletableFuture[_]], tuple: TarantoolTuple) =>
            if (tuples.size >= writeConfig.batchSize) {
              val expectedCount = writeConfig.batchSize
              val future: AsyncTarantoolResult =
                operation(tuples.clone())
                  .exceptionally(toJavaFunction { exception: Throwable =>
                    failedRowsExceptions.add(exception)
                    null
                  })
                  .thenApply(toJavaFunction { result: TarantoolResult[TarantoolTuple] =>
                    if (result.size != expectedCount) {
                      val exception = batchUnsuccessfulException(tuples)
                      failedRowsExceptions.add(exception)
                      throw exception
                    }
                    rowCount.getAndAdd(expectedCount)
                    result
                  })
              futures += future
              tuples.clear()
            }
            tuples += tuple
            futures
        }

    if (tuples.nonEmpty) {
      val expectedCount = tuples.size
      allFutures += operation(tuples)
        .exceptionally(toJavaFunction { exception: Throwable =>
          failedRowsExceptions.add(exception)
          null
        })
        .thenApply(toJavaFunction { result: TarantoolResult[TarantoolTuple] =>
          if (result.size != expectedCount) {
            val exception = batchUnsuccessfulException(tuples)
            failedRowsExceptions.add(exception)
            throw exception
          }
          rowCount.getAndAdd(expectedCount)
          result
        })
    }

    var savedException: Throwable = null
    try {
      CompletableFuture
        .allOf(allFutures: _*)
        .handle(toJavaBiFunction { (_: Void, exception: Throwable) =>
          if (!failedRowsExceptions.isEmpty) {
            val sw: StringWriter = new StringWriter()
            val pw: PrintWriter = new PrintWriter(sw)
            try {
              failedRowsExceptions.foreach { exception =>
                pw.append("\n\n")
                exception.printStackTrace(pw)
              }
              savedException = TarantoolSparkException("Dataset write failed: " + sw.toString)
            } finally {
              pw.close()
            }
          } else {
            if (Option(exception).isDefined) {
              savedException = exception
            } else {
              logInfo(s"Dataset write success, ${rowCount.get()} rows written")
            }
          }
          null
        })
        .join()
    } catch {
      case throwable: Throwable => savedException = throwable
    }

    if (Option(savedException).isDefined) {
      savedException match {
        case e: RuntimeException => throw e
        case e: Any              => throw TarantoolSparkException(e)
      }
    }
  }

  private def batchUnsuccessfulException(
    tuples: ListBuffer[TarantoolTuple]
  ): TarantoolSparkException = {
    val batch = tuples
      .map(tuple => tuple.toMessagePackValue(messagePackMapper).toString)
      .toList
    logError(s"Failed to write next batch $batch because the previous batch writing failed")
    TarantoolSparkException(
      "Not all tuples of the batch were written successfully"
    )
  }
}

object TarantoolWriteRDD {

  def apply(
    sc: SparkContext,
    writeConfig: WriteConfig
  )(
    implicit
    ct: ClassTag[TarantoolTuple]
  ): TarantoolWriteRDD[TarantoolTuple] =
    new TarantoolWriteRDD[TarantoolTuple](
      sc,
      writeConfig.spaceName,
      writeConfig
    )
}
