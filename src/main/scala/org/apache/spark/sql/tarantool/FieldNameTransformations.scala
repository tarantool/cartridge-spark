package org.apache.spark.sql.tarantool

import io.tarantool.spark.connector.util.StringUtils

/**
  * Defines possible conversions of field names for different schema sources.
  *
  * @author Alexey Kuzin
  */
object FieldNameTransformations extends Enumeration {

  case class FieldNameTransformation(transform: String => String) extends super.Val {
    def apply(fieldName: String): String = transform(fieldName)
  }

  implicit def valueToFieldNameTransformation(x: Value): FieldNameTransformation =
    x.asInstanceOf[FieldNameTransformation]

  val NONE: FieldNameTransformation = FieldNameTransformation(identity)
  val SNAKE_CASE: FieldNameTransformation = FieldNameTransformation(StringUtils.camelToSnake)
  val LOWER_CASE: FieldNameTransformation = FieldNameTransformation(s => s.toLowerCase)
  val UPPER_CASE: FieldNameTransformation = FieldNameTransformation(s => s.toUpperCase)
}
