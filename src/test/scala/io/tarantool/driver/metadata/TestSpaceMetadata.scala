package io.tarantool.driver.metadata

import scala.collection.JavaConverters.mapAsJavaMapConverter

/**
  *
  *
  * @author Alexey Kuzin
  */
object TestSpaceMetadata {

  private val spaceFieldMetadata = Map(
    "firstname" -> new TarantoolFieldMetadata("firstname", "string", 0),
    "middlename" -> new TarantoolFieldMetadata("middlename", "string", 1),
    "lastname" -> new TarantoolFieldMetadata("lastname", "string", 2),
    "id" -> new TarantoolFieldMetadata("id", "string", 3),
    "age" -> new TarantoolFieldMetadata("age", "integer", 4),
    "salary" -> new TarantoolFieldMetadata("salary", "decimal", 5),
    "discount" -> new TarantoolFieldMetadata("discount", "number", 6),
    "favourite_constant" -> new TarantoolFieldMetadata("favourite_constant", "number", 7),
    "married" -> new TarantoolFieldMetadata("married", "boolean", 8),
    "updated" -> new TarantoolFieldMetadata("updated", "integer", 9)
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
