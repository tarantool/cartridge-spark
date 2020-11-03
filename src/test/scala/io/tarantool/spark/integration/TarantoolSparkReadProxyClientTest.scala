package io.tarantool.spark.integration

import io.tarantool.driver.api.tuple.TarantoolTuple
import io.tarantool.spark.TarantoolSpark
import io.tarantool.spark.connection.{
  TarantoolConfigBuilder,
  TarantoolConnection
}
import org.scalatest._

class TarantoolSparkReadProxyClientTest
    extends FunSuite
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with SharedSparkContextProxyClient {

  private val SPACE_NAME: String = "test_space"

  test("Create connection") {
    val tarantoolConnection = TarantoolConnection()
    val tarantoolClient = tarantoolConnection.client(
      TarantoolConfigBuilder.createReadOptions(SPACE_NAME, sc.getConf))
    val spaceHolder = tarantoolClient.metadata.getSpaceByName(SPACE_NAME)
    spaceHolder.isPresent should equal(true)
  }

  test("Load the whole space") {
    val rdd = TarantoolSpark.load[TarantoolTuple](sc, "test_space")
    rdd.count() > 0 should equal(true)
  }
}
