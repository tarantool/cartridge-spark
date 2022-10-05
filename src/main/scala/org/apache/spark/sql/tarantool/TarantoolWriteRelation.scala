package org.apache.spark.sql.tarantool

import io.tarantool.driver.api.tuple.TarantoolTuple
import io.tarantool.spark.connector.rdd.TarantoolWriteRDD
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
  * Represents meta information about writable Tarantool space
  *
  * @author Alexey Kuzin
  */
private[spark] case class TarantoolWriteRelation(
  override val sqlContext: SQLContext,
  override val rdd: TarantoolWriteRDD[TarantoolTuple],
  override val userSpecifiedSchema: Option[StructType]
) extends TarantoolBaseRelation(sqlContext, rdd, userSpecifiedSchema)
    with InsertableRelation {

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val dataFrameWriter = data.write.format("org.apache.spark.tarantool.sql")

    if (overwrite) {
      dataFrameWriter.mode(SaveMode.Overwrite).save()
    } else {
      dataFrameWriter.mode(SaveMode.ErrorIfExists).save()
    }
  }
}
