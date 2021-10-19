package org.apache.spark.sql.tarantool

import io.tarantool.driver.api.conditions.Conditions
import io.tarantool.driver.mappers.{DefaultMessagePackMapperFactory, MessagePackMapper}
import io.tarantool.spark.connector.toSparkContextFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.tarantool.MapFunctions.tupleToRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

/**
  * Represents meta information about call to a Tarantool space or a stored function
  *
  * @author Alexey Kuzin
  */
private[spark] case class TarantoolRelation(
  override val sqlContext: SQLContext,
  parameters: Map[String, String],
  userSpecifiedSchema: Option[StructType]
)(
  implicit val tupleMapper: MessagePackMapper =
    DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper()
) extends BaseRelation
    with TableScan {

  @transient private val sparkSession = sqlContext.sparkSession
  @transient private val sc = sqlContext.sparkSession.sparkContext

  private val spaceName = parameters.get("space") match {
    case None       => throw new IllegalArgumentException("space name is not specified in parameters")
    case Some(name) => name
  }

  @volatile private var spaceSchema: StructType = _

  private def getSpaceSchema: StructType = {
    if (spaceSchema == null) {
      synchronized {
        if (spaceSchema == null) {
          spaceSchema = TarantoolSchema(sparkSession).asStructType(spaceName)
        }
      }
    }
    spaceSchema
  }

  override def schema: StructType =
    userSpecifiedSchema.getOrElse(getSpaceSchema)

  override def buildScan(): RDD[Row] = {
    val rdd = sc.tarantoolSpace(spaceName, Conditions.any())
    rdd.map(tuple => tupleToRow(tuple, tupleMapper, schema))
  }
}
