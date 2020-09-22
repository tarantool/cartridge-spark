package io.tarantool.spark.connection

import org.scalatest.{FlatSpec, Matchers}

class TarantoolConnectionSpec extends FlatSpec with Matchers {

  "A Connection" should " be initialized only once" in {
    val conn1 = TarantoolConnection()
    val conn2 = TarantoolConnection()

    conn1 should equal (conn2)
  }
}
