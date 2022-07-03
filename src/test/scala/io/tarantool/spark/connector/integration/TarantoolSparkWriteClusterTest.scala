package io.tarantool.spark.connector.integration

import io.tarantool.driver.api.conditions.Conditions
import io.tarantool.driver.api.tuple.{DefaultTarantoolTupleFactory, TarantoolTuple}
import io.tarantool.driver.exceptions.TarantoolException
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory
import io.tarantool.spark.connector.connection.TarantoolConnection
import io.tarantool.spark.connector.config.TarantoolConfig
import io.tarantool.spark.connector.toSparkContextFunctions
import org.apache.spark.SparkException
import org.apache.spark.sql.{Encoders, Row, SaveMode}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.Tag

import java.util
import java.util.concurrent.ThreadLocalRandom
import scala.collection.JavaConverters.{mapAsJavaMapConverter, seqAsJavaListConverter}

/**
  * @author Alexey Kuzin
  */
@org.scalatest.DoNotDiscover
class TarantoolSparkWriteClusterTest
    extends AnyFunSuite
    with Matchers
    with TarantoolSparkClusterTestSuite {

  private val SPACE_NAME: String = "orders"

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
      .save()

    actual =
      SharedSparkContext.spark.sparkContext.tarantoolSpace(SPACE_NAME, Conditions.any()).collect()
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
      .save()

    actual =
      SharedSparkContext.spark.sparkContext.tarantoolSpace(SPACE_NAME, Conditions.any()).collect()
    actual.length should equal(current)

    actual.foreach(item => item.getString("order_type") should endWith("333"))

    // ErrorIfExists mode checks that partition is empty and provides an exception if it is not
    val thrownException = the[IllegalStateException] thrownBy {
      df.write
        .format("org.apache.spark.sql.tarantool")
        .mode(SaveMode.ErrorIfExists)
        .option("tarantool.space", SPACE_NAME)
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
      .save()

    actual =
      SharedSparkContext.spark.sparkContext.tarantoolSpace(SPACE_NAME, Conditions.any()).collect()
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
      .save()

    actual =
      SharedSparkContext.spark.sparkContext.tarantoolSpace(SPACE_NAME, Conditions.any()).collect()
    actual.length should equal(current)

    actual.foreach(item => item.getString("order_type") should endWith("444"))

    // Clear the data and check if they are written in Ignore mode
    SharedSparkContext.container.executeScript("test_teardown.lua").get()

    df.write
      .format("org.apache.spark.sql.tarantool")
      .mode(SaveMode.Ignore)
      .option("tarantool.space", SPACE_NAME)
      .save()

    actual =
      SharedSparkContext.spark.sparkContext.tarantoolSpace(SPACE_NAME, Conditions.any()).collect()
    actual.length should equal(current)

    actual.foreach(item => item.getString("order_type") should endWith("555"))
  }

  test("should write a Dataset to the space with field names mapping") {
    val space = "test_space"

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

  test("should write a Dataset to the space with decimal values", DecimalTestTag) {
    val space = "reg_numbers"

    SharedSparkContext.spark.sql("create database if not exists dl_raw")
    SharedSparkContext.spark.sql("drop table if exists DL_RAW.reg_numbers")

    SharedSparkContext.spark.sql("""
                                   |create table if not exists DL_RAW.reg_numbers (
                                   |     bucket_id             integer 
                                   |    ,idreg                 decimal(38,18) 
                                   |    ,regnum                decimal(38) 
                                   |  ) stored as orc""".stripMargin)
    SharedSparkContext.spark.sql("""
                                   |insert into dl_raw.reg_numbers values 
                                   |(null, 1085529600000.13452690000413, 404503014700028), 
                                   |(null, 1086629600000.13452690000413, 404503015800028), 
                                   |(null, 1087430400000.13452690000413, 304503016900085) 
                                   |""".stripMargin)

    val ds = SharedSparkContext.spark.table("dl_raw.reg_numbers")

    ds.show(false)
    ds.printSchema()

    ds.write
      .format("org.apache.spark.sql.tarantool")
      .option("tarantool.space", space)
      .mode(SaveMode.Overwrite)
      .save()

    val actual =
      SharedSparkContext.spark.sparkContext.tarantoolSpace(space, Conditions.any()).collect()
    actual.length should equal(3)

    actual(0).getDecimal("idreg") should equal(
      BigDecimal("1085529600000.134526900004130000").bigDecimal
    )
    actual(0).getDecimal("regnum") should equal(BigDecimal("404503014700028").bigDecimal)

    actual(1).getDecimal("idreg") should equal(
      BigDecimal("1086629600000.134526900004130000").bigDecimal
    )
    actual(1).getDecimal("regnum") should equal(BigDecimal("404503015800028").bigDecimal)

    actual(2).getDecimal("idreg") should equal(
      BigDecimal("1087430400000.134526900004130000").bigDecimal
    )
    actual(2).getDecimal("regnum") should equal(BigDecimal("304503016900085").bigDecimal)
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

object DecimalTestTag extends Tag("io.tarantool.spark.integration.DecimalTest")
