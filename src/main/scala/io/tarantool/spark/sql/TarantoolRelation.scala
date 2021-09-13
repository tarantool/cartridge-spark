package io.tarantool.spark.sql

import io.tarantool.driver.api.conditions.Conditions
import io.tarantool.spark.connector.toSparkContextFunctions
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
    val rdd = sqlContext.sparkContext.tarantoolSpace("test_space", Conditions.any())
    rdd.map(row => tupleToRow(row))
  }
}
