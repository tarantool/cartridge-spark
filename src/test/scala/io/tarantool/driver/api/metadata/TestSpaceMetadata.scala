package io.tarantool.driver.api.metadata

import java.util
import java.util.Optional
import java.util.concurrent.CompletableFuture
import scala.collection.JavaConverters.mapAsJavaMapConverter

/**
  * @author Alexey Kuzin
  */

object TestTarantoolMetadata {

  def apply(): TarantoolMetadataOperations = new TarantoolMetadataOperations {
    override def scheduleRefresh(): Unit = {}

    override def refresh(): CompletableFuture[Void] = CompletableFuture.completedFuture(null)

    override def getSpaceByName(spaceName: String): Optional[TarantoolSpaceMetadata] =
      Optional.ofNullable(spaceName match {
        case "testSpace"          => TestSpaceMetadata()
        case "testSpaceWithArray" => TestSpaceWithArrayMetadata()
        case "testSpaceWithMap"   => TestSpaceWithMapMetadata()
        case _                    => null
      })

    override def getIndexByName(spaceId: Int, indexName: String): Optional[TarantoolIndexMetadata] =
      Optional.empty()

    override def getIndexByName(
      spaceName: String,
      indexName: String
    ): Optional[TarantoolIndexMetadata] = Optional.empty()

    override def getIndexById(spaceName: String, indexId: Int): Optional[TarantoolIndexMetadata] =
      Optional.empty()

    override def getIndexById(spaceId: Int, indexId: Int): Optional[TarantoolIndexMetadata] =
      Optional.empty()

    override def getSpaceById(spaceId: Int): Optional[TarantoolSpaceMetadata] = Optional.empty()

    override def getSpaceIndexes(spaceId: Int): Optional[util.Map[String, TarantoolIndexMetadata]] =
      Optional.empty()

    override def getSpaceIndexes(
      spaceName: String
    ): Optional[util.Map[String, TarantoolIndexMetadata]] = Optional.empty()
  }
}

case class TestSpaceMetadata(fieldMetadata: Map[String, TarantoolFieldMetadata]) extends TarantoolSpaceMetadata {
  override def getSpaceId: Int = 0

  override def getOwnerId: Int = 0

  override def getSpaceName: String = "unknown"

  override def getSpaceFormatMetadata: util.Map[String, TarantoolFieldMetadata] =
    fieldMetadata.asJava

  override def getFieldByName(fieldName: String): Optional[TarantoolFieldMetadata] =
    Optional.ofNullable(fieldMetadata.get(fieldName).orNull)

  override def getFieldByPosition(fieldPosition: Int): Optional[TarantoolFieldMetadata] =
    Optional.ofNullable(
      fieldMetadata.values.find(field => field.getFieldPosition == fieldPosition).orNull
    )

  override def getFieldPositionByName(fieldName: String): Int =
    fieldMetadata.get(fieldName).map(field => field.getFieldPosition).getOrElse(-1)

  override def getFieldNameByPosition(fieldPosition: Int): Optional[String] =
    Optional.ofNullable(
      fieldMetadata.values
        .find(field => field.getFieldPosition == fieldPosition)
        .map(field => field.getFieldName)
        .orNull
    )
}

object TestSpaceMetadata {

  private val spaceFieldMetadata = Map(
    "firstname" -> TestFieldMetadata("firstname", "string", 0),
    "middlename" -> TestFieldMetadata("middlename", "string", 1),
    "lastname" -> TestFieldMetadata("lastname", "string", 2),
    "id" -> TestFieldMetadata("id", "string", 3),
    "age" -> TestFieldMetadata("age", "integer", 4),
    "salary" -> TestFieldMetadata("salary", "decimal", 5),
    "discount" -> TestFieldMetadata("discount", "number", 6),
    "favourite_constant" -> TestFieldMetadata("favourite_constant", "double", 7),
    "married" -> TestFieldMetadata("married", "boolean", 8),
    "updated" -> TestFieldMetadata("updated", "unsigned", 9)
  )

  def apply(): TarantoolSpaceMetadata =
    TestSpaceMetadata(spaceFieldMetadata)
}

object TestSpaceWithArrayMetadata {

  private val spaceFieldMetadata = Map(
    "order_id" -> TestFieldMetadata("order_id", "string", 0),
    "order_items" -> TestFieldMetadata("order_items", "array", 1),
    "updated" -> TestFieldMetadata("updated", "integer", 2)
  )

  def apply(): TarantoolSpaceMetadata =
    TestSpaceMetadata(spaceFieldMetadata)
}

object TestSpaceWithMapMetadata {

  private val spaceFieldMetadata = Map(
    "id" -> TestFieldMetadata("order_id", "string", 0),
    "settings" -> TestFieldMetadata("order_items", "map", 1),
    "updated" -> TestFieldMetadata("updated", "integer", 2)
  )

  def apply(): TarantoolSpaceMetadata =
    TestSpaceMetadata(spaceFieldMetadata)
}

case class TestFieldMetadata(
  fieldName: String,
  fieldType: String,
  fieldPosition: Int,
  isNullable: Boolean = true
) extends TarantoolFieldMetadata {
  override def getFieldName: String = fieldName

  override def getFieldType: String = fieldType

  override def getFieldPosition: Int = fieldPosition

  override def getIsNullable: Boolean = isNullable
}
