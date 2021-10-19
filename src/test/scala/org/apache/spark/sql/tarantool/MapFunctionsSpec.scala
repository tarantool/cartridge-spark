package org.apache.spark.sql.tarantool

import io.tarantool.driver.DefaultTarantoolTupleFactory
import io.tarantool.driver.api.tuple.TarantoolTupleImpl
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory
import io.tarantool.driver.metadata.{
  TestSpaceMetadata,
  TestSpaceWithArrayMetadata,
  TestSpaceWithMapMetadata
}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.Inspectors.forAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import scala.collection.JavaConverters.{mapAsJavaMapConverter, seqAsJavaListConverter}

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
      StructField("firstname", StringType, true),
      StructField("middlename", StringType, true),
      StructField("lastname", StringType, true),
      StructField("id", StringType, true),
      StructField("age", IntegerType, true),
      StructField("salary", DecimalType.USER_DEFAULT, true),
      StructField("discount", FloatType, true),
      StructField("favourite_constant", DoubleType, true),
      StructField("married", BooleanType, true),
      StructField("updated", LongType, true)
    )
  )

  private val simpleSchemaWithArray = StructType(
    Array(
      StructField("order_id", StringType, true),
      StructField("order_items", DataTypes.createArrayType(IntegerType), true),
      StructField("updated", LongType, true)
    )
  )

  private val simpleSchemaWithMap = StructType(
    Array(
      StructField("id", StringType, true),
      StructField("settings", DataTypes.createMapType(StringType, StringType), true),
      StructField("updated", LongType, true)
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
}
