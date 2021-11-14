package org.apache.spark.sql.tarantool

import io.tarantool.driver.api.metadata.{
  TestSpaceMetadata,
  TestSpaceWithArrayMetadata,
  TestSpaceWithMapMetadata
}
import io.tarantool.driver.api.tuple.DefaultTarantoolTupleFactory
import io.tarantool.driver.core.tuple.TarantoolTupleImpl
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.Inspectors.forAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import scala.collection.JavaConverters.{
  iterableAsScalaIterableConverter,
  mapAsJavaMapConverter,
  seqAsJavaListConverter
}

/**
  *
  *
  * @author Alexey Kuzin
  */
class MapFunctionsSpec extends AnyFlatSpec with Matchers {

  private val defaultMapper =
    DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper()
  private val tupleFactory = new DefaultTarantoolTupleFactory(defaultMapper)

  private val simpleSchema = StructType(
    Array(
      StructField("firstname", StringType, nullable = true),
      StructField("middlename", StringType, nullable = true),
      StructField("lastname", StringType, nullable = true),
      StructField("id", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true),
      StructField("salary", DecimalType.USER_DEFAULT, nullable = true),
      StructField("discount", FloatType, nullable = true),
      StructField("favourite_constant", DoubleType, nullable = true),
      StructField("married", BooleanType, nullable = true),
      StructField("updated", LongType, nullable = true)
    )
  )

  private val simpleSchemaWithArray = StructType(
    Array(
      StructField("order_id", StringType, nullable = true),
      StructField("order_items", DataTypes.createArrayType(IntegerType), nullable = true),
      StructField("updated", LongType, nullable = true)
    )
  )

  private val simpleSchemaWithMap = StructType(
    Array(
      StructField("id", StringType, nullable = true),
      StructField("settings", DataTypes.createMapType(StringType, StringType), nullable = true),
      StructField("updated", LongType, nullable = true)
    )
  )

  it should "convert an empty tuple to an empty row" in {
    val tuple = tupleFactory.create()
    val row = MapFunctions.tupleToRow(tuple, defaultMapper, simpleSchema)

    row should not be null
    row.length should equal(simpleSchema.length)
    forAll(row.toSeq)(field => Option(field).isEmpty should be(true))
  }

  it should "convert a tuple with simple values of different types" in {
    val time = Instant.now().getEpochSecond
    val tuple = new TarantoolTupleImpl(
      Seq(
        "Akakiy",
        "Akakievich",
        "Ivanov",
        null,
        38,
        new java.math.BigDecimal(200),
        0.5f,
        Math.PI,
        false,
        time
      ).asJava,
      defaultMapper,
      TestSpaceMetadata()
    )
    val row = MapFunctions.tupleToRow(tuple, defaultMapper, simpleSchema)
    val expected = Row(
      "Akakiy",
      "Akakievich",
      "Ivanov",
      null,
      38,
      new java.math.BigDecimal(200),
      0.5f,
      Math.PI,
      false,
      time
    )

    row should not be null
    row.length should equal(simpleSchema.length)

    forAll(row.toSeq.zip(expected.toSeq))(tuple => tuple._1 should equal(tuple._2))
  }

  it should "convert a tuple with array values" in {
    val time = Instant.now().getEpochSecond
    val tuple = new TarantoolTupleImpl(
      Seq(
        null,
        defaultMapper.toValue(List(1, 2, 3, 4).asJava),
        time
      ).asJava,
      defaultMapper,
      TestSpaceWithArrayMetadata()
    )
    val row = MapFunctions.tupleToRow(tuple, defaultMapper, simpleSchemaWithArray)
    val expected = Row(
      null,
      List(1, 2, 3, 4).asJava,
      time
    )

    row should not be null
    row.length should equal(simpleSchemaWithArray.length)

    forAll(row.toSeq.zip(expected.toSeq))(tuple => tuple._1 should equal(tuple._2))
  }

  it should "convert a tuple with map values" in {
    val time = Instant.now().getEpochSecond
    val tuple = new TarantoolTupleImpl(
      Seq(
        null,
        defaultMapper.toValue(Map("host" -> "127.0.0.1", "port" -> "3301").asJava),
        time
      ).asJava,
      defaultMapper,
      TestSpaceWithMapMetadata()
    )
    val row = MapFunctions.tupleToRow(tuple, defaultMapper, simpleSchemaWithMap)
    val expected = Row(
      null,
      Map("host" -> "127.0.0.1", "port" -> "3301").asJava,
      time
    )

    row should not be null
    row.length should equal(simpleSchemaWithMap.length)

    forAll(row.toSeq.zip(expected.toSeq))(tuple => tuple._1 should equal(tuple._2))
  }

  it should "convert an empty row to an empty tuple" in {
    val row = Row()
    val tuple = MapFunctions.rowToTuple(tupleFactory, row)

    tuple should not be null
    tuple.size should equal(0)
  }

  it should "convert a row with simple values of different types" in {
    val time = Instant.now().getEpochSecond
    val row = Row(
      "Akakiy",
      "Akakievich",
      "Ivanov",
      null,
      38,
      new java.math.BigDecimal(200),
      0.5f,
      Math.PI,
      false,
      time
    )

    val tuple = MapFunctions.rowToTuple(tupleFactory, row)

    val expected = new TarantoolTupleImpl(
      Seq(
        "Akakiy",
        "Akakievich",
        "Ivanov",
        null,
        38,
        new java.math.BigDecimal(200),
        0.5f,
        Math.PI,
        false,
        time
      ).asJava,
      defaultMapper,
      TestSpaceMetadata()
    )

    tuple should not be null
    tuple.size should equal(simpleSchema.length)

    for ((f1, f2) <- tuple.asScala.toSeq.zip(expected.asScala.toSeq)) {
      f1 should equal(f2)
    }
  }

  it should "convert a row with array values" in {
    val time = Instant.now().getEpochSecond
    val row = Row(
      null,
      List(1, 2, 3, 4).asJava,
      time
    )

    val tuple = MapFunctions.rowToTuple(tupleFactory, row)
    val expected = new TarantoolTupleImpl(
      Seq(
        null,
        List(1, 2, 3, 4).asJava,
        time
      ).asJava,
      defaultMapper,
      TestSpaceWithArrayMetadata()
    )

    tuple should not be null
    tuple.size should equal(simpleSchemaWithArray.length)

    for ((f1, f2) <- tuple.asScala.toSeq.zip(expected.asScala.toSeq)) {
      f1 should equal(f2)
    }
  }

  it should "convert a row with map values" in {
    val time = Instant.now().getEpochSecond
    val row = Row(
      null,
      Map("host" -> "127.0.0.1", "port" -> "3301").asJava,
      time
    )

    val tuple = MapFunctions.rowToTuple(tupleFactory, row)
    val expected = new TarantoolTupleImpl(
      Seq(
        null,
        Map("host" -> "127.0.0.1", "port" -> "3301").asJava,
        time
      ).asJava,
      defaultMapper,
      TestSpaceWithMapMetadata()
    )

    tuple should not be null
    tuple.size should equal(simpleSchemaWithMap.length)

    for ((f1, f2) <- tuple.asScala.toSeq.zip(expected.asScala.toSeq)) {
      f1 should equal(f2)
    }
  }
}
