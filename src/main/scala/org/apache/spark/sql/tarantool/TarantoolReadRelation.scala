package org.apache.spark.sql.tarantool

import io.tarantool.driver.api.tuple.TarantoolTuple
import io.tarantool.driver.mappers.MessagePackMapper
import io.tarantool.driver.mappers.factories.DefaultMessagePackMapperFactory
import io.tarantool.spark.connector.rdd.TarantoolReadRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.TableScan
import org.apache.spark.sql.tarantool.MapFunctions.tupleToRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

/**
  * Represents meta information about readable Tarantool space
  *
  * @author Alexey Kuzin
  */
private[spark] case class TarantoolReadRelation(
  override val sqlContext: SQLContext,
  override val rdd: TarantoolReadRDD[TarantoolTuple],
  override val userSpecifiedSchema: Option[StructType]
)(
  implicit val tupleMapper: MessagePackMapper =
    DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper()
) extends TarantoolBaseRelation(sqlContext, rdd, userSpecifiedSchema)
    with TableScan {

  override def buildScan(): RDD[Row] =
    rdd.map(tuple => tupleToRow(tuple, tupleMapper, schema))
}
