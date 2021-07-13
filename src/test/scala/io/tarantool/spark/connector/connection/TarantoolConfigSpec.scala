package io.tarantool.spark.connector.connection

import io.tarantool.driver.TarantoolServerAddress
import io.tarantool.driver.auth.SimpleTarantoolCredentials
import io.tarantool.spark.connector.config.{Credentials, ReadConfig, TarantoolConfig, Timeouts}
import io.tarantool.spark.connector.partition.TarantoolSinglePartitioner
import org.apache.spark.SparkConf
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TarantoolConfigSpec extends AnyFlatSpec with Matchers {

  it should "apply default settings" in {
    val sparkConf = new SparkConf()

    val rConf: ReadConfig = ReadConfig()
    rConf.partitioner should not be None
    rConf.batchSize should equal(1000)

    val tConf: TarantoolConfig = TarantoolConfig(sparkConf)

    tConf.hosts should equal(Array(new TarantoolServerAddress("127.0.0.1:3301")))
    tConf.credentials should equal(None)
    tConf.timeouts should equal(Timeouts(None, None, None))
  }

  it should "apply default settings with timeouts" in {
    val sparkConf = new SparkConf()
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
