package io.tarantool.spark.connector.integration

import io.tarantool.driver.api.conditions.Conditions
import io.tarantool.spark.connector.toSparkContextFunctions
import org.apache.spark.sql.SaveMode
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Tag
import org.scalatest.Suite

import java.math.MathContext

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

  private def generateDecimalRational(i: Integer): String =
    String.valueOf(10855296 + i) + "00000.134526900004130000"

  private def generateDecimalInteger(i: Integer): String =
    "40450" + String.valueOf(30147 + i) + "00028"

  private def generateRows(): String =
    Range(0, 100)
      .map(i => s"|(null, ${generateDecimalRational(i)}, ${generateDecimalInteger(i)})")
      .mkString(",\n")

  val variants =
    Table(
      ("stopOnError", "batchSize"),
      (false, 9),
      (false, 10),
      (false, 100),
      (false, 101),
      (true, 9),
      (true, 10),
      (true, 100),
      (true, 101)
    )

  test("should write a Dataset to the space with decimal values", DecimalTestTag) {
    forAll(variants) { (stopOnError, batchSize) =>
      val space = "reg_numbers"

      SharedSparkContext.spark.sql("create database if not exists dl_raw")
      SharedSparkContext.spark.sql("drop table if exists DL_RAW.reg_numbers")

      SharedSparkContext.spark.sql("""
                                     |create table if not exists DL_RAW.reg_numbers (
                                     |     bucket_id             integer 
                                     |    ,idreg                 decimal(38,18) 
                                     |    ,regnum                decimal(38) 
                                     |  ) stored as orc""".stripMargin)
      SharedSparkContext.spark.sql(s"""
                                      |insert into dl_raw.reg_numbers values 
                                       ${generateRows}
                                      |""".stripMargin)

      val ds = SharedSparkContext.spark.table("dl_raw.reg_numbers")

      ds.printSchema()

      ds.write
        .format("org.apache.spark.sql.tarantool")
        .option("tarantool.space", space)
        .option("tarantool.stopOnError", stopOnError)
        .option("tarantool.batchSize", batchSize)
        .mode(SaveMode.Overwrite)
        .save()

      val actual =
        SharedSparkContext.spark.sparkContext.tarantoolSpace(space, Conditions.any()).collect()
      actual.length should equal(100)

      for (i <- 0 until 100) {
        val tuple = actual(i)
        tuple.getDecimal("idreg") should equal(
          BigDecimal(generateDecimalRational(i)).bigDecimal
        )
        tuple.getDecimal("regnum") should equal(BigDecimal(generateDecimalInteger(i)).bigDecimal)
      }

      val df = SharedSparkContext.spark.read
        .format("org.apache.spark.sql.tarantool")
        .option("tarantool.space", space)
        .load()

      df.count() should equal(100)
      var count = 0
      df.select("regnum").rdd.map(row => row.get(0)).collect().foreach { row =>
        row should equal(
          BigDecimal(generateDecimalInteger(count) + ".000000000000000000", MathContext.DECIMAL128).bigDecimal
        )
        count += 1
      }
    }
  }
}

object DecimalTestTag extends Tag("io.tarantool.spark.integration.DecimalTest")
