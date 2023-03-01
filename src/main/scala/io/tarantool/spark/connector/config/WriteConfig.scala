package io.tarantool.spark.connector.config

import org.apache.spark.SparkConf
import org.apache.spark.sql.tarantool.FieldNameTransformations
import org.apache.spark.sql.tarantool.FieldNameTransformations.FieldNameTransformation

case class WriteConfig(
  spaceName: String,
  batchSize: Int = WriteConfig.DEFAULT_BATCH_SIZE,
  stopOnError: Boolean = true,
  rollbackOnError: Boolean = true,
  transformFieldNames: FieldNameTransformation = FieldNameTransformations.NONE
) extends Serializable

object WriteConfig extends TarantoolConfigBase {

  private val SPACE_NAME = "space"
  private val BATCH_SIZE = "batchSize"
  private val STOP_ON_ERROR = "stopOnError"
  private val ROLLBACK_ON_ERROR = "rollbackOnError"
  private val TRANSFORM_FIELD_NAMES = "transformFieldNames"
  private val DEFAULT_BATCH_SIZE = 1000

  def apply(sparkConf: SparkConf, options: Option[Map[String, String]]): WriteConfig =
    new WriteConfig(
      spaceName = getFromSparkConfOrOptions(sparkConf, options, SPACE_NAME) match {
        case None       => throw new IllegalArgumentException("space name is not specified in parameters")
        case Some(name) => name
      },
      batchSize = getFromSparkConfOrOptions(sparkConf, options, BATCH_SIZE)
        .map(_.toInt)
        .getOrElse(DEFAULT_BATCH_SIZE),
      stopOnError = getFromSparkConfOrOptions(sparkConf, options, STOP_ON_ERROR)
        .map(_.toBoolean)
        .getOrElse(true),
      rollbackOnError = getFromSparkConfOrOptions(sparkConf, options, ROLLBACK_ON_ERROR)
        .map(_.toBoolean)
        .getOrElse(true),
      transformFieldNames = getFromSparkConfOrOptions(sparkConf, options, TRANSFORM_FIELD_NAMES)
        .map(s => FieldNameTransformations.withName(s.toUpperCase))
        .getOrElse(FieldNameTransformations.NONE)
    )

  def apply(spaceName: String): WriteConfig =
    new WriteConfig(spaceName)
}
