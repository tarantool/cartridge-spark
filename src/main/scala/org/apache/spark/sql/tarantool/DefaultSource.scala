package org.apache.spark.sql.tarantool

import io.tarantool.driver.api.tuple.TarantoolTuple
import io.tarantool.spark.connector.config.{ReadConfig, WriteConfig}
import io.tarantool.spark.connector.connection.TarantoolConnection
import io.tarantool.spark.connector.rdd.{TarantoolReadRDD, TarantoolWriteRDD}
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
  * DataSourceV2 implementation for Tarantool
  *
  * @author Alexey Kuzin
  */
class DefaultSource
    extends DataSourceRegister
    with RelationProvider
    with SchemaRelationProvider
    with CreatableRelationProvider {
  override def shortName(): String = "tarantool"

  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String]
  ): BaseRelation =
    constructReadRelation(sqlContext, parameters, None)

  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String],
    schema: StructType
  ): BaseRelation =
    constructReadRelation(sqlContext, parameters, Some(schema))

  override def createRelation(
    sqlContext: SQLContext,
    mode: SaveMode,
    parameters: Map[String, String],
    data: DataFrame
  ): BaseRelation = {
    val relation = constructWriteRelation(sqlContext, parameters, Some(data.schema))
    val connection = TarantoolConnection()
    sqlContext.sparkContext.addSparkListener(new SparkListener() {
      override def onApplicationEnd(end: SparkListenerApplicationEnd): Unit =
        connection.close()
    })

    mode match {
      case SaveMode.Append => relation.rdd.write(connection, data, overwrite = true)
      case SaveMode.Overwrite => {
        relation.rdd.truncate(connection)
        relation.rdd.write(connection, data, overwrite = false)
      }
      case SaveMode.ErrorIfExists => {
        if (relation.rdd.nonEmpty(connection)) {
          throw new IllegalStateException(
            "SaveMode is set to ErrorIfExists and dataframe " +
              "already exists in Tarantool and contains data."
          )
        }
        relation.rdd.write(connection, data, overwrite = false)
      }
      case SaveMode.Ignore =>
        if (relation.rdd.isEmpty(connection)) {
          relation.rdd.write(connection, data, overwrite = false)
        }
    }

    relation
  }

  private def constructReadRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String],
    schema: Option[StructType]
  ): TarantoolReadRelation = {
    val readConfig = ReadConfig(sqlContext.sparkContext.getConf, Some(parameters))

    TarantoolReadRelation(
      sqlContext,
      TarantoolReadRDD(
        sqlContext.sparkContext,
        readConfig
      ),
      schema
    )
  }

  private def constructWriteRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String],
    schema: Option[StructType]
  ): TarantoolWriteRelation = {
    val writeConfig = WriteConfig(sqlContext.sparkContext.getConf, Some(parameters))

    TarantoolWriteRelation(
      sqlContext,
      TarantoolWriteRDD(
        sqlContext.sparkContext,
        writeConfig
      ),
      schema
    )
  }
}
