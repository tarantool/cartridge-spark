package io.tarantool.spark.connector.integration

import io.tarantool.driver.api.conditions.Conditions
import io.tarantool.driver.api.tuple.{DefaultTarantoolTupleFactory, TarantoolTuple}
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory
import io.tarantool.spark.connector.config.TarantoolConfig
import io.tarantool.spark.connector.connection.TarantoolConnection
import io.tarantool.spark.connector.rdd.converter.FunctionBasedTupleConverterFactory
import io.tarantool.spark.connector.toSparkContextFunctions
import org.scalatest._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters.seqAsJavaListConverter

class TarantoolSparkReadClusterClientTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with SharedSparkContext {

  //space format:
  // s = box.schema.space.create('_spark_test_space')
  // s:format({{name = 'id', type = 'unsigned'}, {name = 'name', type = 'string'}, {name = 'value', type = 'unsigned'}})
  // s:create_index('primary', {type = 'tree', parts = {'id'}})

  private val SPACE_NAME: String = "test_space"

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    container.executeScript("test_setup.lua").get
  }

  override protected def afterEach(): Unit = {
    container.executeScript("test_teardown.lua").get
    super.afterEach()
  }

  test("Create connection") {
    val tarantoolConnection = TarantoolConnection()
    val tarantoolClient = Option(
      tarantoolConnection.client(TarantoolConfig(sc.getConf))
    )
    tarantoolClient should not be Option.empty
    val spaceHolder = tarantoolClient.get.metadata.getSpaceByName(SPACE_NAME)
    spaceHolder.isPresent should equal(true)
    spaceHolder.get.getSpaceFormatMetadata.size > 0 should equal(true)
  }

  test("Load the whole space") {
    val rdd: Array[TarantoolTuple] =
      sc.tarantoolSpace("test_space", Conditions.any()).collect()
    rdd.length > 0 should equal(true)
  }

  test("Load the whole space into a DataFrame") {
    val df = spark.read
      .format("org.apache.spark.sql.tarantool")
      .option("tarantool.space", "test_space")
      .load()

    df.count() > 0 should equal(true)
    df.select("id").rdd.map(row => row.get(0)).collect() should equal(Array(1, 2, 3))
  }

  test("Load the whole space with conditions") {
    val mapper = DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper();
    val startTuple = new DefaultTarantoolTupleFactory(mapper).create(List(1).asJava)
    val cond: Conditions = Conditions
      .indexGreaterThan("id", List(1).asJava)
      .withLimit(2)
      .startAfter(startTuple)
    val rdd: Array[TarantoolTuple] = sc.tarantoolSpace("test_space", cond).collect()

    rdd.length should equal(2)
    rdd.apply(0).getInteger("id") should equal(2)
    rdd.apply(1).getInteger("id") should equal(3)
  }

  test("Load the whole space with tuple converter") {
    implicit val tupleConverterFactory: FunctionBasedTupleConverterFactory[Book] =
      FunctionBasedTupleConverterFactory { t =>
        val book = new Book()
        book.id = t.getInteger("id")
        book.name = t.getString("name")
        book.author = t.getString("author")
        book.year = t.getInteger("year")
        book
      }
    val rdd: Array[Book] =
      sc.tarantoolSpace[Book]("test_space", Conditions.any()).collect()
    rdd.length > 0 should equal(true)
    rdd.find(b => b.year == 1605) should not be None
  }
}
