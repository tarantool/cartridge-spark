package io.tarantool.spark.connector.connection

import io.tarantool.driver.exceptions.{TarantoolClientException, TarantoolConnectionException}
import io.tarantool.spark.connector.config.TarantoolConfig
import io.tarantool.spark.connector.integration.SharedSparkContext
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TarantoolConnectionSpec extends AnyFlatSpec with Matchers with SharedSparkContext {

  "Client in Connection" should " be initialized only once" in {
    val conf: TarantoolConfig = TarantoolConfig(sc.getConf)
    val conn1 = TarantoolConnection(conf)
    val conn2 = TarantoolConnection(conf)
    conn1 should equal(conn2)

    val client1 = conn1.client()
    client1 should equal(conn1.client())

    val client2 = conn2.client()
    client1 should equal(client2)

    conn1.close()
    val client3 = conn1.client()
    (client3 should not).equal(client1)
    (client3 should not).equal(client2)
  }

  "Client in Connection" should " be closed when the Connection closes" in {
    val conf: TarantoolConfig = TarantoolConfig(sc.getConf)
    val conn = TarantoolConnection(conf)
    val client = conn.client()

    client.getVersion should not be null

    conn.close()

    val exception = intercept[TarantoolClientException] {
      client.getVersion
    }
    exception.getCause.getClass should equal(classOf[TarantoolConnectionException])
  }
}
