package io.tarantool.spark.connector.integration

import io.tarantool.driver.api.conditions.Conditions
import io.tarantool.driver.api.tuple.{DefaultTarantoolTupleFactory, TarantoolTuple}
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory
import io.tarantool.spark.connector.toSparkContextFunctions
import org.apache.spark.sql.{Encoders, Row}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.util
import java.util.concurrent.ThreadLocalRandom
import scala.collection.JavaConverters.{mapAsJavaMapConverter, seqAsJavaListConverter}

/**
  * @author Alexey Kuzin
  */
class TarantoolSparkWriteClusterTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with SharedSparkContext {

  private val SPACE_NAME: String = "orders"

  private val orderSchema = Encoders.product[Order].schema

  test("should write a list of objects to the space") {

    val orders = Range(1, 10).map(i => Order(i))

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(orders.map(order => order.asRow())),
      orderSchema
    )

    df.write
      .format("org.apache.spark.sql.tarantool")
      .mode("overwrite")
      .option("tarantool.space", SPACE_NAME)
      .save()

    val actual = spark.sparkContext.tarantoolSpace(SPACE_NAME, Conditions.any()).collect()

    actual.length should be > 0
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
  }

}

case class Order(
  id: Int,
  bucketId: Int,
  orderType: String,
  orderValue: BigDecimal,
  orderItems: List[Int],
  options: Map[String, String],
  cleared: Boolean
) {

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