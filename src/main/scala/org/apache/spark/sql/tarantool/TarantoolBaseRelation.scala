package org.apache.spark.sql.tarantool

import io.tarantool.driver.api.tuple.TarantoolTuple
import io.tarantool.spark.connector.rdd.TarantoolBaseRDD
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SQLContext

/**
  * Basic class for BaseRelation implementations
  *
  * @author Alexey Kuzin
  */
private[spark] abstract class TarantoolBaseRelation(
  override val sqlContext: SQLContext,
  val rdd: TarantoolBaseRDD,
  val userSpecifiedSchema: Option[StructType]
) extends BaseRelation
    with Serializable {

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

  override def schema: StructType =
    userSpecifiedSchema.getOrElse(getSpaceSchema)
}
