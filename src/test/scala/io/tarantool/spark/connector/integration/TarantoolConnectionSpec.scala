package io.tarantool.spark.connector.integration

import io.tarantool.spark.connector.config.TarantoolConfig
import io.tarantool.spark.connector.connection.TarantoolConnection
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * @author Alexey Kuzin
  */
@org.scalatest.DoNotDiscover
class TarantoolConnectionSpec
    extends AnyFlatSpec
    with Matchers
    with TarantoolSparkClusterTestSuite {

  "Client in Connection" should " be initialized only once" in {
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
}
