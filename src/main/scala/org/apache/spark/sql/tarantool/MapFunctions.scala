package org.apache.spark.sql.tarantool

import io.tarantool.driver.api.tuple.{TarantoolField, TarantoolTuple, TarantoolTupleFactory}
import io.tarantool.driver.mappers.MessagePackValueMapper
import io.tarantool.spark.connector.util.StringUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.tarantool.FieldNameTransformations.FieldNameTransformation
import org.apache.spark.sql.types._

import java.lang.{
  Boolean => JBoolean,
  Byte => JByte,
  Character => JCharacter,
  Double => JDouble,
  Float => JFloat,
  Integer => JInteger,
  Long => JLong,
  Short => JShort
}
import java.util.{ArrayList => JList, HashMap => JMap}
import java.sql.Timestamp
import java.time.Instant
import scala.collection.JavaConverters.{mapAsJavaMapConverter, seqAsJavaListConverter}

/**
  * Contains methods for mapping Tarantool tuples to Spark DataSet rows
  *
  * @author Alexey Kuzin
  */
object MapFunctions {

  @transient private lazy val tupleNamesCache: scala.collection.mutable.Map[String, String] =
    scala.collection.concurrent.TrieMap()

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
  ): Any = {
    val javaType = Some(dataTypeToJavaClass(dataType))
    if (javaType.isEmpty) {
      throw new RuntimeException(s"$dataType is not supported for conversion")
    }
    tupleField.getValue(javaType.get, mapper)
  }

  def dataTypeToJavaClass(dataType: DataType): Class[_] =
    dataType match {
      case StringType       => classOf[java.lang.String]
      case LongType         => classOf[java.lang.Long]
      case IntegerType      => classOf[java.lang.Integer]
      case ShortType        => classOf[java.lang.Integer]
      case ByteType         => classOf[java.lang.Integer]
      case BooleanType      => classOf[java.lang.Boolean]
      case DoubleType       => classOf[java.lang.Double]
      case FloatType        => classOf[java.lang.Float]
      case _: DecimalType   => classOf[java.math.BigDecimal]
      case _: TimestampType => classOf[java.time.Instant]
      case mapType: MapType =>
        val keyClass = dataTypeToJavaClass(mapType.keyType)
        val valueClass = dataTypeToJavaClass(mapType.valueType)
        classOf[java.util.Map[keyClass.type, valueClass.type]]
      case arrayType: ArrayType =>
        val valueClass = dataTypeToJavaClass(arrayType.elementType)
        classOf[java.util.List[valueClass.type]]
    }

  def rowToTuple(tupleFactory: TarantoolTupleFactory, row: Row, transform: FieldNameTransformation): TarantoolTuple =
    Option(row.schema) match {
      case Some(schema) => rowWithSchemaToTuple(tupleFactory, row, transform)
      case None         => rowWithoutSchemaToTuple(tupleFactory, row)
    }

  def rowWithSchemaToTuple(
    tupleFactory: TarantoolTupleFactory,
    row: Row,
    transform: FieldNameTransformation
  ): TarantoolTuple = {
    val tuple = tupleFactory.create()
    row.getValuesMap[Any](row.schema.fieldNames).foreach { (pair) =>
      tuple.putObject(transformSchemaFieldName(pair._1, transform), mapToJavaValue(Option(pair._2)).orNull)
    }
    tuple
  }

  def transformSchemaFieldName(fieldName: String, transform: FieldNameTransformation): String =
    tupleNamesCache.getOrElseUpdate(fieldName, transform(fieldName))

  def rowWithoutSchemaToTuple(tupleFactory: TarantoolTupleFactory, row: Row): TarantoolTuple = {
    val tuple = tupleFactory.create(
      row.toSeq
        .map(value => mapToJavaValue(Option(value)))
        .map(nullableValue => nullableValue.orNull)
        .asJava
    )
    tuple
  }

  def mapToJavaValue(value: Option[Any]): Option[Any] =
    if (value.isDefined) {
      Option(
        value.get match {
          case value: Map[_, _]   => mapMapValue(value)
          case value: Iterable[_] => mapIterableValue(value)
          case value: Any         => mapSimpleValue(value)
        }
      )
    } else {
      Option.empty
    }

  def mapMapValue[K, V](value: Map[_, _]): JMap[K, V] =
    new JMap[K, V](
      value.toSeq
        .map(tuple =>
          Tuple2(
            mapToJavaValue(Option(tuple._1)).orNull.asInstanceOf[K],
            mapToJavaValue(Option(tuple._2)).orNull.asInstanceOf[V]
          )
        )
        .toMap
        .asJava
    )

  def mapIterableValue[V](value: Iterable[_]): JList[V] = {
    val javaList = new JList[V](value.size)
    javaList.addAll(
      value.map(item => mapToJavaValue(Option(item)).orNull.asInstanceOf[V]).toSeq.asJava
    )
    javaList
  }

  def mapSimpleValue(value: Any): Any =
    value match {
      case value: BigInt     => value
      case value: BigDecimal => value
      case value: Boolean    => value.booleanValue().asInstanceOf[JBoolean]
      case value: Byte       => value.asInstanceOf[JByte]
      case value: Char       => value.asInstanceOf[JCharacter]
      case value: Short      => value.asInstanceOf[JShort]
      case value: Int        => value.asInstanceOf[JInteger]
      case value: Long       => value.asInstanceOf[JLong]
      case value: Float      => value.asInstanceOf[JFloat]
      case value: Double     => value.asInstanceOf[JDouble]
      case value: Timestamp  => value.toInstant().asInstanceOf[Instant]
      case value: Any        => identity(value)
    }
}
