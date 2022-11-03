package io.tarantool.spark.connector.integration

import io.tarantool.driver.core.RetryingTarantoolTupleClient
import io.tarantool.spark.connector.config.TarantoolConfig
import io.tarantool.spark.connector.connection.TarantoolConnection
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * @author Alexey Kuzin
  */
@org.scalatest.DoNotDiscover
class TarantoolConnectionSpec extends AnyFlatSpec with Matchers with TarantoolSparkClusterTestSuite {

  "Client in TarantoolConnection" should " be initialized only once" in {
    val conn1 = TarantoolConnection()
    val conn2 = TarantoolConnection()

    val conf: TarantoolConfig = TarantoolConfig(SharedSparkContext.sc.getConf)

    conn1.client(conf) should equal(conn1.client(conf))
    conn1.client(conf) should equal(conn2.client(conf))
    val client = conn1.client(conf)

    conn1.close()
    conn2.client(conf) should (not(equal(client)))
    conn2.close()
  }

  "Client in TarantoolConnection" should " have the specified settings" in {
    val sparkConf = SharedSparkContext.sc.getConf
      .clone()
      .set("tarantool.space", "test_space")
      .set("tarantool.connectTimeout", "10")
      .set("tarantool.readTimeout", "20")
      .set("tarantool.requestTimeout", "30")
      .set("tarantool.connections", "3")
    val tConf: TarantoolConfig = TarantoolConfig(sparkConf)
    val conn = TarantoolConnection()
    val client = conn.client(tConf)
    val actualConfig = client.getConfig()

    actualConfig.getConnectTimeout() should equal(10)
    actualConfig.getReadTimeout() should equal(20)
    actualConfig.getRequestTimeout() should equal(30)
    actualConfig.getConnections() should equal(3)

    conn.close()
  }

  "Client in TarantoolConnection" should " have the retry attempts settings if specified" in {
    val sparkConf = SharedSparkContext.sc.getConf
      .clone()
      .set("tarantool.space", "test_space")
      .set("tarantool.retries.errorType", "network")
      .set("tarantool.retries.maxAttempts", "10")
      .set("tarantool.retries.delay", "100")
    val tConf: TarantoolConfig = TarantoolConfig(sparkConf)
    val conn = TarantoolConnection()
    val client = conn.client(tConf)
    client.isInstanceOf[RetryingTarantoolTupleClient] should be(true)

    conn.close()
  }
}
