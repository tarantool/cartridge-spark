package io.tarantool.spark.connector.integration

import io.tarantool.driver.api.conditions.Conditions
import io.tarantool.spark.connector.toSparkContextFunctions
import org.apache.spark.sql.SaveMode
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Tag
import org.scalatest.Suite

/**
  * @author Alexey Kuzin
  */
@org.scalatest.DoNotDiscover
class TarantoolSparkWriteClusterWithHiveTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterEach { this: Suite =>

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    SharedSparkContext.setupSpark(true)
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    SharedSparkContext.teardownSpark()
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

    val df = SharedSparkContext.spark.read
      .format("org.apache.spark.sql.tarantool")
      .option("tarantool.space", space)
      .load()

    df.count() > 0 should equal(true)
    df.select("regnum").rdd.map(row => row.get(0)).collect() should equal(
      Array(
        BigDecimal("404503014700028.000000000000000000").bigDecimal,
        BigDecimal("404503015800028.000000000000000000").bigDecimal,
        BigDecimal("304503016900085.000000000000000000").bigDecimal
      )
    )
  }
}

object DecimalTestTag extends Tag("io.tarantool.spark.integration.DecimalTest")
