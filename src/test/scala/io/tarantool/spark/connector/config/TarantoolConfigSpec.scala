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
    rConf.spaceName should equal("test_space")
    rConf.conditions should equal(Conditions.any())
    rConf.partitioner should not be None
    rConf.batchSize should equal(1000)

    val wConf: WriteConfig = WriteConfig("test_space")
    wConf.spaceName should equal("test_space")
    wConf.batchSize should equal(1000)
    wConf.timeout should equal(1000)
    wConf.stopOnError should equal(true)
    wConf.rollbackOnError should equal(true)

    val tConf: TarantoolConfig = TarantoolConfig(sparkConf)

    tConf.hosts should equal(Array(new TarantoolServerAddress("127.0.0.1:3301")))
    tConf.credentials should equal(None)
    tConf.timeouts should equal(Timeouts(None, None, None))
    tConf.connections should equal(None)
    tConf.retries should equal(None)
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
      .set("tarantool.connections", "123")
    val tConf: TarantoolConfig = TarantoolConfig(sparkConf)
    tConf.hosts should equal(Array(new TarantoolServerAddress("127.0.0.3:3303")))
    tConf.credentials should equal(Some(Credentials("aaaaa", "bbbbb")))
    tConf.timeouts should equal(Timeouts(Some(10), Some(20), Some(30)))
    tConf.connections should equal(Some(123))
  }

  it should "apply retries settings" in {
    var sparkConf = new SparkConf()
      .set("tarantool.space", "test_space")
      .set("tarantool.retries.errorType", "none")
    var tConf: TarantoolConfig = TarantoolConfig(sparkConf)
    tConf.retries should equal(Some(Retries(ErrorTypes.NONE, None, None)))

    sparkConf = new SparkConf()
      .set("tarantool.space", "test_space")
      .set("tarantool.retries.errorType", "network")
      .set("tarantool.retries.maxAttempts", "10")
      .set("tarantool.retries.delay", "100")
    tConf = TarantoolConfig(sparkConf)
    tConf.retries should equal(Some(Retries(ErrorTypes.NETWORK, Some(10), Some(100))))

    sparkConf = new SparkConf()
      .set("tarantool.space", "test_space")
      .set("tarantool.retries.errorType", "network")
    var ex = intercept[Exception] {
      tConf = TarantoolConfig(sparkConf)
    }
    ex.getMessage should include("Number of retry attempts")

    sparkConf = new SparkConf()
      .set("tarantool.space", "test_space")
      .set("tarantool.retries.errorType", "network")
      .set("tarantool.retries.maxAttempts", "10")
    ex = intercept[Exception] {
      tConf = TarantoolConfig(sparkConf)
    }
    ex.getMessage should include("Delay between retry attempts must be specified")
  }

  it should "apply operation timeout settings" in {
    var sparkConf = new SparkConf()
      .set("tarantool.space", "test_space")
    var wConf = WriteConfig(sparkConf, None)
    wConf.spaceName should equal("test_space")
    wConf.timeout should equal(1000)

    val options = Map(
      "tarantool.timeout" -> "10"
    )
    wConf = WriteConfig(sparkConf, Some(options))
    wConf.timeout should equal(10)

    sparkConf = new SparkConf()
      .set("tarantool.space", "test_space")
      .set("tarantool.timeout", "20")
    wConf = WriteConfig(sparkConf, Some(options))
    wConf.timeout should equal(10)

    wConf = WriteConfig(sparkConf, None)
    wConf.timeout should equal(20)
  }
}
