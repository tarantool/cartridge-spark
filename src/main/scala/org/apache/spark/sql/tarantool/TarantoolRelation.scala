package org.apache.spark.sql.tarantool

import io.tarantool.driver.api.tuple.TarantoolTuple
import io.tarantool.driver.mappers.{DefaultMessagePackMapperFactory, MessagePackMapper}
import io.tarantool.spark.connector.rdd.TarantoolRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, TableScan}
import org.apache.spark.sql.tarantool.MapFunctions.tupleToRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}

/**
  * Represents meta information about call to a Tarantool space or a stored function
  *
  * @author Alexey Kuzin
  */
private[spark] case class TarantoolRelation(
  override val sqlContext: SQLContext,
  rdd: TarantoolRDD[TarantoolTuple],
  userSpecifiedSchema: Option[StructType]
)(
  implicit val tupleMapper: MessagePackMapper =
    DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper()
) extends BaseRelation
    with TableScan
    with InsertableRelation {

  @transient private lazy val sparkSession = sqlContext.sparkSession

  @volatile private var spaceSchema: StructType = _

  private def getSpaceSchema: StructType = {
    if (spaceSchema == null) {
      synchronized {
        if (spaceSchema == null) {
          spaceSchema = TarantoolSchema(sparkSession).asStructType(rdd.space)
        }
      }
    }
    spaceSchema
  }

  def isEmpty: Boolean = rdd.isEmpty()

  def nonEmpty: Boolean = !isEmpty

  override def schema: StructType =
    userSpecifiedSchema.getOrElse(getSpaceSchema)

  override def buildScan(): RDD[Row] =
    rdd.map(tuple => tupleToRow(tuple, tupleMapper, schema))

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val dataFrameWriter = data.write.format("org.apache.spark.tarantool.sql")

    if (overwrite) {
      dataFrameWriter.mode(SaveMode.Overwrite).save()
    } else {
      dataFrameWriter.mode(SaveMode.ErrorIfExists).save()
    }
  }
}
