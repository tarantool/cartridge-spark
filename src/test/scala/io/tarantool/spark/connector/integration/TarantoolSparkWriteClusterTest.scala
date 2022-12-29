package io.tarantool.spark.connector.integration

import io.tarantool.driver.api.conditions.Conditions
import io.tarantool.driver.api.tuple.{DefaultTarantoolTupleFactory, TarantoolTuple}
import io.tarantool.driver.mappers.factories.DefaultMessagePackMapperFactory
import io.tarantool.spark.connector.toSparkContextFunctions
import org.apache.spark.SparkException
import org.apache.spark.sql.{Encoders, Row, SaveMode}
import org.scalatest.Tag
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util
import java.util.concurrent.ThreadLocalRandom
import scala.collection.JavaConverters.{mapAsJavaMapConverter, seqAsJavaListConverter}

/**
  * @author Alexey Kuzin
  */
@org.scalatest.DoNotDiscover
class TarantoolSparkWriteClusterTest extends AnyFunSuite with Matchers with TarantoolSparkClusterTestSuite {

  private val SPACE_NAME: String = "orders"
  private val SNAKE_CASE: String = "snake_case"

  private val orderSchema = Encoders.product[Order].schema

  test("should write a dataset of objects to the specified space with different modes") {

    val orders = Range(1, 10).map(i => Order(i))

    var df = SharedSparkContext.spark.createDataFrame(
      SharedSparkContext.spark.sparkContext.parallelize(orders.map(order => order.asRow())),
      orderSchema
    )

    // Insert, the partition is empty at first
    df.write
      .format("org.apache.spark.sql.tarantool")
      .mode(SaveMode.Append)
      .option("tarantool.space", SPACE_NAME)
      .option("tarantool.transformFieldNames", SNAKE_CASE)
      .save()

    var actual =
      SharedSparkContext.spark.sparkContext.tarantoolSpace(SPACE_NAME, Conditions.any()).collect()
    actual.length should be > 0
    val current = actual.length

    val sorted = actual.sorted[TarantoolTuple](new Ordering[TarantoolTuple]() {
      override def compare(x: TarantoolTuple, y: TarantoolTuple): Int =
        x.getInteger("id").compareTo(y.getInteger("id"))
    })
    for ((expectedItem, actualItem) <- orders.map(o => o.asTuple()).zip(sorted)) {
      actualItem.getInteger("id") should equal(expectedItem.getInteger(0))
      actualItem.getInteger("bucket_id") should not be null
      actualItem.getString("order_type") should equal(expectedItem.getString(2))
      actualItem
        .getObject("order_value", classOf[java.math.BigDecimal])
        .get()
        .doubleValue() should equal(
        expectedItem
          .getObject(3, classOf[java.math.BigDecimal])
          .get()
          .doubleValue() +- 1e-5
      )
      actualItem.getList("order_items") should equal(
        expectedItem.getObject(4, classOf[util.ArrayList[Integer]]).get()
      )
      actualItem.getMap("options") should equal(
        expectedItem.getObject(5, classOf[util.HashMap[String, String]]).get()
      )
      actualItem.getBoolean("cleared") should equal(expectedItem.getBoolean(6))
    }

    // Replace
    df = SharedSparkContext.spark.createDataFrame(
      SharedSparkContext.spark.sparkContext.parallelize(
        orders
          .map(order => order.changeOrderType(order.orderType + "222"))
          .map(order => order.asRow())
      ),
      orderSchema
    )

    df.write
      .format("org.apache.spark.sql.tarantool")
      .mode(SaveMode.Overwrite)
      .option("tarantool.space", SPACE_NAME)
      .option("tarantool.transformFieldNames", SNAKE_CASE)
      .save()

    actual = SharedSparkContext.spark.sparkContext.tarantoolSpace(SPACE_NAME, Conditions.any()).collect()
    actual.length should equal(current)

    actual.foreach(item => item.getString("order_type") should endWith("222"))

    df = SharedSparkContext.spark.createDataFrame(
      SharedSparkContext.spark.sparkContext.parallelize(
        orders
          .map(order => order.changeOrderType(order.orderType + "333"))
          .map(order => order.asRow())
      ),
      orderSchema
    )

    // Second insert with the same IDs does not result in an exception
    df.write
      .format("org.apache.spark.sql.tarantool")
      .mode(SaveMode.Append)
      .option("tarantool.space", SPACE_NAME)
      .option("tarantool.transformFieldNames", SNAKE_CASE)
      .save()

    actual = SharedSparkContext.spark.sparkContext.tarantoolSpace(SPACE_NAME, Conditions.any()).collect()
    actual.length should equal(current)

    actual.foreach(item => item.getString("order_type") should endWith("333"))

    // ErrorIfExists mode checks that partition is empty and provides an exception if it is not
    val thrownException = the[IllegalStateException] thrownBy {
      df.write
        .format("org.apache.spark.sql.tarantool")
        .mode(SaveMode.ErrorIfExists)
        .option("tarantool.space", SPACE_NAME)
        .option("tarantool.transformFieldNames", SNAKE_CASE)
        .save()
    }
    thrownException.getMessage should include("already exists in Tarantool")

    // Clear the data and check that they are written in ErrorIfExists mode
    SharedSparkContext.container.executeScript("test_teardown.lua").get()

    df = SharedSparkContext.spark.createDataFrame(
      SharedSparkContext.spark.sparkContext.parallelize(
        orders
          .map(order => order.changeOrderType(order.orderType + "444"))
          .map(order => order.asRow())
      ),
      orderSchema
    )

    df.write
      .format("org.apache.spark.sql.tarantool")
      .mode(SaveMode.ErrorIfExists)
      .option("tarantool.space", SPACE_NAME)
      .option("tarantool.transformFieldNames", SNAKE_CASE)
      .save()

    actual = SharedSparkContext.spark.sparkContext.tarantoolSpace(SPACE_NAME, Conditions.any()).collect()
    actual.length should equal(current)

    actual.foreach(item => item.getString("order_type") should endWith("444"))

    // Check that new data are not written in Ignore mode if the partition is not empty
    df = SharedSparkContext.spark.createDataFrame(
      SharedSparkContext.spark.sparkContext.parallelize(
        orders
          .map(order => order.changeOrderType(order.orderType + "555"))
          .map(order => order.asRow())
      ),
      orderSchema
    )

    df.write
      .format("org.apache.spark.sql.tarantool")
      .mode(SaveMode.Ignore)
      .option("tarantool.space", SPACE_NAME)
      .option("tarantool.transformFieldNames", SNAKE_CASE)
      .save()

    actual = SharedSparkContext.spark.sparkContext.tarantoolSpace(SPACE_NAME, Conditions.any()).collect()
    actual.length should equal(current)

    actual.foreach(item => item.getString("order_type") should endWith("444"))

    // Clear the data and check if they are written in Ignore mode
    SharedSparkContext.container.executeScript("test_teardown.lua").get()

    df.write
      .format("org.apache.spark.sql.tarantool")
      .mode(SaveMode.Ignore)
      .option("tarantool.space", SPACE_NAME)
      .option("tarantool.transformFieldNames", SNAKE_CASE)
      .save()

    actual = SharedSparkContext.spark.sparkContext.tarantoolSpace(SPACE_NAME, Conditions.any()).collect()
    actual.length should equal(current)

    actual.foreach(item => item.getString("order_type") should endWith("555"))
  }

  test("should write a Dataset to the space with field names mapping with transformation") {
    val space = "test_space"

    // NOTE! The following query actually creates 3 separate RDDs, each containing one row.
    // The last row duplicate will not be removed.
    // This is NOT a normal way of writing data into Spark!
    var ds = SharedSparkContext.spark.sql(
      """
        |select 1 as id, null as bucketId, 'Don Quixote' as bookName, 'Miguel de Cervantes' as author, 1605 as year union all
        |select 2, null, 'The Great Gatsby', 'F. Scott Fitzgerald', 1925 union all
        |select 2, null, 'War and Peace', 'Leo Tolstoy', 1869
        |""".stripMargin
    )

    val ex = intercept[SparkException] {
      ds.write
        .format("org.apache.spark.sql.tarantool")
        .mode(SaveMode.Append)
        .option("tarantool.space", space)
        .option("tarantool.transformFieldNames", SNAKE_CASE)
        .save()
    }
    ex.getMessage should include(
      "Tuple field 3 (unique_key) type does not match one required by operation: expected string, got nil"
    )

    ds = SharedSparkContext.spark.sql(
      """
        |select 1 as id, null as bucketId, 'Miguel de Cervantes' as author, 1605 as year, 'Don Quixote' as bookName, 'lolkek' as uniqueKey union all
        |select 2, null, 'F. Scott Fitzgerald', 1925, 'The Great Gatsby', 'lolkek1' union all
        |select 3, null, 'Leo Tolstoy', 1869, 'War and Peace', 'lolkek2'
        |""".stripMargin
    )

    ds.write
      .format("org.apache.spark.sql.tarantool")
      .mode(SaveMode.Append)
      .option("tarantool.space", space)
      .option("tarantool.transformFieldNames", SNAKE_CASE)
      .save()

    val actual =
      SharedSparkContext.spark.sparkContext.tarantoolSpace(space, Conditions.any()).collect()
    actual.length should equal(3)

    actual(0).getString("author") should equal("Miguel de Cervantes")
    actual(0).getString("book_name") should equal("Don Quixote")
    actual(0).getInteger("year") should equal(1605)
    actual(0).getString("unique_key") should equal("lolkek")

    actual(1).getString("author") should equal("F. Scott Fitzgerald")
    actual(1).getString("book_name") should equal("The Great Gatsby")
    actual(1).getInteger("year") should equal(1925)
    actual(1).getString("unique_key") should equal("lolkek1")

    actual(2).getString("author") should equal("Leo Tolstoy")
    actual(2).getString("book_name") should equal("War and Peace")
    actual(2).getInteger("year") should equal(1869)
    actual(2).getString("unique_key") should equal("lolkek2")
  }

  test("should write a Dataset to the space with field names mapping without transformation") {
    val space = "test_space"

    val ds = SharedSparkContext.spark.sql(
      """
        |select 1 as id, null as bucket_id, 'Miguel de Cervantes' as author, 1605 as year, 'Don Quixote' as book_name, 'lolkek' as unique_key union all
        |select 2, null, 'F. Scott Fitzgerald', 1925, 'The Great Gatsby', 'lolkek1' union all
        |select 3, null, 'Leo Tolstoy', 1869, 'War and Peace', 'lolkek2'
        |""".stripMargin
    )

    ds.write
      .format("org.apache.spark.sql.tarantool")
      .mode(SaveMode.Append)
      .option("tarantool.space", space)
      .option("tarantool.transformFieldNames", "none")
      .save()

    val actual =
      SharedSparkContext.spark.sparkContext.tarantoolSpace(space, Conditions.any()).collect()
    actual.length should equal(3)

    actual(0).getString("author") should equal("Miguel de Cervantes")
    actual(0).getString("book_name") should equal("Don Quixote")
    actual(0).getInteger("year") should equal(1605)
    actual(0).getString("unique_key") should equal("lolkek")

    actual(1).getString("author") should equal("F. Scott Fitzgerald")
    actual(1).getString("book_name") should equal("The Great Gatsby")
    actual(1).getInteger("year") should equal(1925)
    actual(1).getString("unique_key") should equal("lolkek1")

    actual(2).getString("author") should equal("Leo Tolstoy")
    actual(2).getString("book_name") should equal("War and Peace")
    actual(2).getInteger("year") should equal(1869)
    actual(2).getString("unique_key") should equal("lolkek2")
  }

  test(
    "should fail fast and drop records if one batch failed when stopOnError is true",
    testTags = WriteTestTag
  ) {
    val spark = SharedSparkContext.spark
    import spark.implicits._

    val space = "test_space"

    val ds = Seq(
      BookWithKey(Some(1), Option.empty[Integer], "Miguel de Cervantes", 1605, "Don Quixote", "lolkek"),
      BookWithKey(Option.empty[Integer], Option.empty[Integer], "Miguel de Cervantes", 1605, "Don Quixote", "lolkek"),
      BookWithKey(
        Some(3),
        Option.empty[Integer],
        "F. Scott Fitzgerald",
        1925,
        "The Great Gatsby",
        "lolkek1"
      )
    ).toDS

    val ex = intercept[SparkException] {
      ds.write
        .format("org.apache.spark.sql.tarantool")
        .mode(SaveMode.Overwrite)
        .option("tarantool.space", space)
        .option("tarantool.stopOnError", true)
        .option("tarantool.rollbackOnError", true)
        .option("tarantool.batchSize", 2)
        .save()
    }
    ex.getMessage should not(contain("Not all tuples of the batch"))

    val actual = spark.sparkContext.tarantoolSpace(space, Conditions.any()).collect()
    actual.length should equal(1) // only one tuple from the first batch should be successful

    actual(0).getString("author") should equal("Miguel de Cervantes")
    actual(0).getString("book_name") should equal("Don Quixote")
    actual(0).getInteger("year") should equal(1605)
    actual(0).getString("unique_key") should equal("lolkek")
  }

  test(
    "should not fail fast if one batch failed when stopOnError is false",
    testTags = WriteTestTag
  ) {
    val spark = SharedSparkContext.spark
    import spark.implicits._

    val space = "test_space"

    val ds = Seq(
      BookWithKey(Some(1), Option.empty[Integer], "Miguel de Cervantes", 1605, "Don Quixote", "lolkek"),
      BookWithKey(Option.empty[Integer], Option.empty[Integer], "Miguel de Cervantes", 1605, "Don Quixote", "lolkek"),
      BookWithKey(
        Some(3),
        Option.empty[Integer],
        "F. Scott Fitzgerald",
        1925,
        "The Great Gatsby",
        "lolkek1"
      )
    ).toDS

    intercept[SparkException] {
      ds.write
        .format("org.apache.spark.sql.tarantool")
        .mode(SaveMode.Overwrite)
        .option("tarantool.space", space)
        .option("tarantool.stopOnError", false)
        .option("tarantool.batchSize", 1)
        .save()
    }

    val actual = spark.sparkContext.tarantoolSpace(space, Conditions.any()).collect()
    actual.length should equal(2)

    actual(0).getString("author") should equal("Miguel de Cervantes")
    actual(0).getString("book_name") should equal("Don Quixote")
    actual(0).getInteger("year") should equal(1605)
    actual(0).getString("unique_key") should equal("lolkek")

    actual(1).getString("author") should equal("F. Scott Fitzgerald")
    actual(1).getString("book_name") should equal("The Great Gatsby")
    actual(1).getInteger("year") should equal(1925)
    actual(1).getString("unique_key") should equal("lolkek1")
  }

  test("should throw an exception if the space name is not specified") {
    assertThrows[IllegalArgumentException] {
      val orders = Range(1, 10).map(i => Order(i))

      val df = SharedSparkContext.spark.createDataFrame(
        SharedSparkContext.spark.sparkContext.parallelize(orders.map(order => order.asRow())),
        orderSchema
      )

      df.write
        .format("org.apache.spark.sql.tarantool")
        .mode(SaveMode.Overwrite)
        .save()
    }
  }
}

case class Order(
  id: Int,
  bucketId: Int,
  var orderType: String,
  orderValue: BigDecimal,
  orderItems: List[Int],
  options: Map[String, String],
  cleared: Boolean
) {

  def changeOrderType(newOrderType: String): Order = {
    orderType = newOrderType
    this
  }

  def asRow(): Row =
    Row(id, bucketId, orderType, orderValue, orderItems, options, cleared)

  private def asJavaList[V](aList: List[_]): util.List[V] = {
    val javaList = new util.ArrayList[V](aList.size)
    javaList.addAll(aList.map(v => v.asInstanceOf[V]).asJava)
    javaList.asInstanceOf[util.List[V]]
  }

  def asTuple(): TarantoolTuple =
    Order.tupleFactory.create(
      id.asInstanceOf[Integer],
      bucketId.asInstanceOf[Integer],
      orderType,
      orderValue.underlying(),
      asJavaList(orderItems),
      new util.HashMap[String, String](options.asJava),
      cleared.asInstanceOf[java.lang.Boolean]
    )
}

object Order {

  private val tupleFactory = new DefaultTarantoolTupleFactory(
    DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper()
  )
  private val random: ThreadLocalRandom = ThreadLocalRandom.current()

  def apply(id: Int): Order =
    Order(
      id,
      random.nextInt(1, 3),
      "type" + random.nextInt(1, 3),
      BigDecimal.valueOf(random.nextDouble()),
      List(1, 2, 3),
      Map("segment" -> "vip", "system" -> "internal"),
      cleared = true
    )
}

case class BookWithKey(
  id: Option[Integer],
  bucketId: Option[Integer],
  author: String,
  year: Integer,
  bookName: String,
  uniqueKey: String
)

object WriteTestTag extends Tag("io.tarantool.spark.integration.WriteTest")
