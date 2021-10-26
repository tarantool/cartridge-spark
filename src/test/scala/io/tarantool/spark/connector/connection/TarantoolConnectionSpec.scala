package io.tarantool.spark.connector.connection

import io.tarantool.spark.connector.config.TarantoolConfig
import io.tarantool.spark.connector.integration.SharedSparkContext
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TarantoolConnectionSpec extends AnyFlatSpec with Matchers with SharedSparkContext {

  "Client in Connection" should " be initialized only once" in {
    val conn1 = TarantoolConnection()
    val conn2 = TarantoolConnection()

    val conf: TarantoolConfig = TarantoolConfig(sc.getConf)

    conn1.client(conf) should equal(conn1.client(conf))
    (conn1.client(conf) should not).equal(conn2.client(conf))
  }
}
