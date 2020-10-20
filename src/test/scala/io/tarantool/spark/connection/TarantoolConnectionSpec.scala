package io.tarantool.spark.connection

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TarantoolConnectionSpec extends AnyFlatSpec with Matchers {

  "A Connection" should " be initialized only once" in {
    val conn1 = TarantoolConnection()
    val conn2 = TarantoolConnection()

    conn1 should equal (conn2)
  }
}
