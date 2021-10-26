package org.apache.spark.sql.tarantool

import io.tarantool.spark.connector.config.ReadConfig
import io.tarantool.spark.connector.rdd.TarantoolRDD
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
    constructRelation(sqlContext, parameters, None)

  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String],
    schema: StructType
  ): BaseRelation =
    constructRelation(sqlContext, parameters, Some(schema))

  private def constructRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String],
    schema: Option[StructType]
  ): TarantoolRelation = {
    val readConfig = ReadConfig(sqlContext.sparkContext.getConf, Some(parameters))

    TarantoolRelation(
      sqlContext,
      TarantoolRDD(
        sqlContext.sparkContext,
        readConfig
      ),
      schema
    )
  }
}
