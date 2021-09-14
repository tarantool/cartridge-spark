package io.tarantool.spark.sql

import io.tarantool.driver.api.conditions.Conditions
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory
import io.tarantool.spark.connector.toSparkContextFunctions
import io.tarantool.spark.sql.MapFunctions.tupleToRow
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

/**
  * Represents meta information about call to a Tarantool space or a stored function
  *
  * @author Alexey Kuzin
  */
private[spark] case class TarantoolRelation(
  @transient val sqlContext: SQLContext,
  parameters: Map[String, String],
  userSpecifiedSchema: Option[StructType]
) extends BaseRelation
    with TableScan {

  override def schema: StructType =
    userSpecifiedSchema.getOrElse(TarantoolSchema(sqlContext.sparkContext, parameters))

  override def buildScan(): RDD[Row] = {
    val spaceName = parameters.get("space") match {
      case None       => throw new IllegalArgumentException("space is not specified in parameters")
      case Some(name) => name
    }

    val rdd = sqlContext.sparkContext.tarantoolSpace(spaceName, Conditions.any())
    val defaultMapper = DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper()
    rdd.map(tuple => tupleToRow(tuple, defaultMapper, schema))
  }
}
