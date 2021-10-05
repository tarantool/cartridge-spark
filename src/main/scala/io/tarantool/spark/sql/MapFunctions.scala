package io.tarantool.spark.sql

import io.tarantool.driver.api.tuple.{TarantoolField, TarantoolTuple}
import io.tarantool.driver.mappers.MessagePackValueMapper
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._

/**
  * Contains methods for mapping Tarantool tuples to Spark DataSet rows
  *
  * @author Alexey Kuzin
  */
private[spark] object MapFunctions {

  def tupleToRow(
    tuple: TarantoolTuple,
    mapper: MessagePackValueMapper,
    schema: StructType,
    requiredColumns: Array[String] = Array.empty[String]
  ): Row = {
    val values: Array[(Any, StructField)] = schema.fields.map { field =>
      val tupleField = tuple.getField(field.name)
      tupleField.isPresent match {
        case true  => (convertToDataType(tupleField.get, field.dataType, mapper), field)
        case false => (null, field)
      }
    }

    val requiredValues = requiredColumns.nonEmpty match {
      case true =>
        val requiredValueMap = Map(values.collect({
          case (rowValue, rowField) if requiredColumns.contains(rowField.name) =>
            (rowField.name, (rowValue, rowField))
        }): _*)
        requiredColumns.collect({ case name => requiredValueMap.getOrElse(name, null) })
      case false => values
    }

    new GenericRowWithSchema(
      requiredValues.map(_._1),
      DataTypes.createStructType(requiredValues.map(_._2))
    )
  }

  def convertToDataType(
    tupleField: TarantoolField,
    dataType: DataType,
    mapper: MessagePackValueMapper
  ) = {
    val javaType = Some(dataTypeToJavaClass(dataType))
    if (javaType.isEmpty) {
      throw new RuntimeException(s"$dataType is not supported for conversion")
    }
    tupleField.getValue(javaType.get, mapper)
  }

  def dataTypeToJavaClass(dataType: DataType): Class[_] =
    dataType match {
      case StringType     => classOf[java.lang.String]
      case LongType       => classOf[java.lang.Long]
      case IntegerType    => classOf[java.lang.Integer]
      case ShortType      => classOf[java.lang.Integer]
      case ByteType       => classOf[java.lang.Integer]
      case BooleanType    => classOf[java.lang.Boolean]
      case DoubleType     => classOf[java.lang.Double]
      case FloatType      => classOf[java.lang.Float]
      case _: DecimalType => classOf[java.math.BigDecimal]
      case mapType: MapType =>
        val keyClass = dataTypeToJavaClass(mapType.keyType)
        val valueClass = dataTypeToJavaClass(mapType.valueType)
        classOf[java.util.Map[keyClass.type, valueClass.type]]
      case arrayType: ArrayType =>
        val valueClass = dataTypeToJavaClass(arrayType.elementType)
        classOf[java.util.List[valueClass.type]]
    }
}
