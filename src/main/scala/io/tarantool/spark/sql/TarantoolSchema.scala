package io.tarantool.spark.sql

import io.tarantool.driver.metadata.TarantoolSpaceMetadata
import io.tarantool.spark.connector.config.TarantoolConfig
import io.tarantool.spark.connector.connection.TarantoolConnection
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}

import java.util.function.Supplier
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.language.implicitConversions

/**
  * Provides schema loading mechanism and conversion into StructType
  *
  * @author Alexey Kuzin
  */
object TarantoolSchema {

  def apply(sc: SparkContext, parameters: Map[String, String]): StructType = {
    val config = TarantoolConfig(sc.getConf)
    val conn = TarantoolConnection()
    val client = conn.client(config)

    val spaceName = parameters.get("space") match {
      case None       => throw new IllegalArgumentException("space is not specified in parameters")
      case Some(name) => name
    }

    val spaceMetadata = client
      .metadata()
      .getSpaceByName(spaceName)
      .orElseThrow(new Supplier[Throwable] {
        override def get(): Throwable =
          new RuntimeException(s"No metadata found for space $spaceName")
      })

    asStructType(spaceMetadata)
  }

  def asStructType(metadata: TarantoolSpaceMetadata): StructType =
    DataTypes.createStructType(
      metadata.getSpaceFormatMetadata.asScala.toSeq
        .map(e =>
          DataTypes.createStructField(
            e._2.getFieldName,
            TarantoolFieldTypes.withNameLowerCase(e._2.getFieldType).dataType,
            true
          )
        )
        .toArray
    )
}

object TarantoolFieldTypes extends Enumeration {
  // TODO: Warning: map and array types are very rough, and the others may also not fit into the actual data,
  // due to the MessagePack optimizations and lack of the field type information in Tarantool

  final case class TarantoolFieldType(name: String, dataType: DataType) extends super.Val

  val ANY: TarantoolFieldType = TarantoolFieldType(
    "any",
    DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true)
  )

  val UNSIGNED: TarantoolFieldType = TarantoolFieldType("unsigned", DataTypes.LongType)
  val STRING: TarantoolFieldType = TarantoolFieldType("string", DataTypes.StringType)
  val NUMBER: TarantoolFieldType = TarantoolFieldType("number", DataTypes.DoubleType)
  val DOUBLE: TarantoolFieldType = TarantoolFieldType("double", DataTypes.DoubleType)
  val INTEGER: TarantoolFieldType = TarantoolFieldType("integer", DataTypes.LongType)
  val BOOLEAN: TarantoolFieldType = TarantoolFieldType("boolean", DataTypes.BooleanType)
  val DECIMAL: TarantoolFieldType = TarantoolFieldType("decimal", DataTypes.createDecimalType())
  val UUID: TarantoolFieldType = TarantoolFieldType("uuid", DataTypes.StringType)

  val ARRAY: TarantoolFieldType =
    TarantoolFieldType("array", DataTypes.createArrayType(DataTypes.StringType, true))

  val MAP: TarantoolFieldType = TarantoolFieldType(
    "map",
    DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true)
  )

  def withNameLowerCase(name: String): Value =
    values
      .find(_.toString.toLowerCase() == name.toLowerCase())
      .getOrElse(throw new NoSuchElementException(s"No value found for '$name'"))

  implicit def convert(value: Value): TarantoolFieldType = value.asInstanceOf[TarantoolFieldType]
}
