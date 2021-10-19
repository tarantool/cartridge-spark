package io.tarantool.driver.metadata

import java.util
import java.util.Optional
import java.util.concurrent.CompletableFuture
import scala.collection.JavaConverters.mapAsJavaMapConverter

/**
  *
  *
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

object TestSpaceMetadata {

  private val spaceFieldMetadata = Map(
    "firstname" -> new TarantoolFieldMetadata("firstname", "string", 0),
    "middlename" -> new TarantoolFieldMetadata("middlename", "string", 1),
    "lastname" -> new TarantoolFieldMetadata("lastname", "string", 2),
    "id" -> new TarantoolFieldMetadata("id", "string", 3),
    "age" -> new TarantoolFieldMetadata("age", "integer", 4),
    "salary" -> new TarantoolFieldMetadata("salary", "decimal", 5),
    "discount" -> new TarantoolFieldMetadata("discount", "number", 6),
    "favourite_constant" -> new TarantoolFieldMetadata("favourite_constant", "double", 7),
    "married" -> new TarantoolFieldMetadata("married", "boolean", 8),
    "updated" -> new TarantoolFieldMetadata("updated", "unsigned", 9)
  )

  def apply(): TarantoolSpaceMetadata = {
    val metadata = new TarantoolSpaceMetadata()
    metadata.setSpaceFormatMetadata(spaceFieldMetadata.asJava)
    metadata
  }
}

object TestSpaceWithArrayMetadata {

  private val spaceFieldMetadata = Map(
    "order_id" -> new TarantoolFieldMetadata("order_id", "string", 0),
    "order_items" -> new TarantoolFieldMetadata("order_items", "array", 1),
    "updated" -> new TarantoolFieldMetadata("updated", "integer", 2)
  )

  def apply(): TarantoolSpaceMetadata = {
    val metadata = new TarantoolSpaceMetadata()
    metadata.setSpaceFormatMetadata(spaceFieldMetadata.asJava)
    metadata
  }
}

object TestSpaceWithMapMetadata {

  private val spaceFieldMetadata = Map(
    "id" -> new TarantoolFieldMetadata("order_id", "string", 0),
    "settings" -> new TarantoolFieldMetadata("order_items", "map", 1),
    "updated" -> new TarantoolFieldMetadata("updated", "integer", 2)
  )

  def apply(): TarantoolSpaceMetadata = {
    val metadata = new TarantoolSpaceMetadata()
    metadata.setSpaceFormatMetadata(spaceFieldMetadata.asJava)
    metadata
  }
}
