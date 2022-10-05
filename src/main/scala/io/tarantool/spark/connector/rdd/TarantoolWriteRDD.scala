package io.tarantool.spark.connector.rdd

import io.tarantool.driver.api.conditions.Conditions
import io.tarantool.driver.api.tuple.{DefaultTarantoolTupleFactory, TarantoolTuple}
import io.tarantool.driver.api.TarantoolResult
import io.tarantool.driver.mappers.{DefaultMessagePackMapperFactory, MessagePackMapper}
import io.tarantool.spark.connector.{Logging, TarantoolSparkException}
import io.tarantool.spark.connector.config.{TarantoolConfig, WriteConfig}
import io.tarantool.spark.connector.connection.TarantoolConnection
import io.tarantool.spark.connector.util.ScalaToJavaHelper.{toJavaBiFunction, toJavaFunction}
import org.apache.spark.sql.tarantool.MapFunctions.rowToTuple
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.SparkContext

import java.io.{PrintWriter, StringWriter}
import java.util.concurrent.CompletableFuture
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

        var rowCount: Long = 0
        val failedRowsExceptions: ListBuffer[Throwable] = ListBuffer()
        val allFutures =
          partition
            .map {
              row =>
                val future: CompletableFuture[TarantoolResult[TarantoolTuple]] = {
                  if (overwrite) {
                    client.space(space).replace(rowToTuple(tupleFactory, row))
                  } else {
                    client.space(space).insert(rowToTuple(tupleFactory, row))
                  }
                }
                future
                  .exceptionally(toJavaFunction { exception: Throwable =>
                    failedRowsExceptions += exception
                    null
                  })
                  .thenApply(toJavaFunction { result: TarantoolResult[TarantoolTuple] =>
                    rowCount += 1
                    result
                  })
            }
            .toArray[CompletableFuture[_]]

        var savedException: Throwable = null
        try {
          CompletableFuture
            .allOf(allFutures: _*)
            .handle(toJavaBiFunction {
              (_: Void, exception: Throwable) =>
                if (failedRowsExceptions.nonEmpty) {
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
                    logInfo(s"Dataset write success, $rowCount rows written")
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
    )
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
