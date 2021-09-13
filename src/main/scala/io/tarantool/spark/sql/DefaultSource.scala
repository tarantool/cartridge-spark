package io.tarantool.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{
  BaseRelation,
  DataSourceRegister,
  RelationProvider,
  SchemaRelationProvider
}
import org.apache.spark.sql.types.StructType

/**
  * DataSourceV2 implementation for Tarantool
  *
  * @author Alexey Kuzin
  */
class DefaultSource extends DataSourceRegister with RelationProvider with SchemaRelationProvider {
  override def shortName(): String = "tarantool"

  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String]
  ): BaseRelation =
    new TarantoolRelation(sqlContext, parameters, userSpecifiedSchema = None)

  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String],
    schema: StructType
  ): BaseRelation =
    new TarantoolRelation(sqlContext, parameters, userSpecifiedSchema = Some(schema))
}
