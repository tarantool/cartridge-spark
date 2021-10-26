package io.tarantool.spark.connector.config

import io.tarantool.driver.api.TarantoolServerAddress
import io.tarantool.driver.api.conditions.Conditions
import org.apache.spark.SparkConf
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters.seqAsJavaListConverter

class TarantoolConfigSpec extends AnyFlatSpec with Matchers {

  it should "apply default settings" in {
    val sparkConf = new SparkConf()

    val rConf: ReadConfig = ReadConfig("test_space")
    rConf.conditions should equal(Conditions.any())
    rConf.partitioner should not be None
    rConf.batchSize should equal(1000)

    val tConf: TarantoolConfig = TarantoolConfig(sparkConf)

    tConf.hosts should equal(Array(new TarantoolServerAddress("127.0.0.1:3301")))
    tConf.credentials should equal(None)
    tConf.timeouts should equal(Timeouts(None, None, None))
  }

  it should "apply settings from options with priority" in {
    val sparkConf = new SparkConf()
      .set("tarantool.connectTimeout", "10")

    val options = Map(
      "tarantool.space" -> "test_space",
      "tarantool.connectTimeout" -> "10"
    )

    var rConf = ReadConfig(sparkConf, Some(options))
    rConf.spaceName should equal("test_space")

    rConf = rConf.withConditions(Conditions.indexGreaterThan(0, List(1).asJava))
    rConf.conditions should equal(Conditions.indexGreaterThan(0, List(1).asJava))

    val tConf: TarantoolConfig = TarantoolConfig(sparkConf)
    tConf.timeouts should equal(Timeouts(Some(10), None, None))
  }

  it should "throw an exception when space name is missing" in {
    val sparkConf = new SparkConf()
      .set("tarantool.connectTimeout", "10")

    assertThrows[IllegalArgumentException](
      ReadConfig(sparkConf, None)
    )
  }

  it should "apply default settings with timeouts" in {
    val sparkConf = new SparkConf()
      .set("tarantool.space", "test_space")
      .set("tarantool.connectTimeout", "10")
      .set("tarantool.readTimeout", "20")
      .set("tarantool.requestTimeout", "30")
      .set("tarantool.hosts", "127.0.0.3:3303")
      .set("tarantool.username", "aaaaa")
      .set("tarantool.password", "bbbbb")
    val tConf: TarantoolConfig = TarantoolConfig(sparkConf)
    tConf.hosts should equal(Array(new TarantoolServerAddress("127.0.0.3:3303")))
    tConf.credentials should equal(Some(Credentials("aaaaa", "bbbbb")))
    tConf.timeouts should equal(Timeouts(Some(10), Some(20), Some(30)))
  }
}
