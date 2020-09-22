package io.tarantool.spark.integration

import io.tarantool.driver.TarantoolClient
import io.tarantool.driver.api.tuple.TarantoolTuple
import io.tarantool.spark.TarantoolSpark
import io.tarantool.spark.connection.{TarantoolConfigBuilder, TarantoolConnection}
import org.scalatest._

class TarantoolSparkReadClusterClientTest extends FunSuite
  with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with SharedSparkContextClusterClient {

  //space format:
  // s = box.schema.space.create('_spark_test_space')
  // s:format({{name = 'id', type = 'unsigned'}, {name = 'name', type = 'string'}, {name = 'value', type = 'unsigned'}})
  // s:create_index('primary', {type = 'tree', parts = {'id'}})

  private val SPACE_NAME: String = "_spark_test_space"
  private var tarantoolClient: TarantoolClient = _

  test("Create connection") {
    val tarantoolConnection = TarantoolConnection()
    tarantoolClient = tarantoolConnection.client(TarantoolConfigBuilder.createReadOptions(SPACE_NAME, sc.getConf))
    tarantoolClient.metadata.refresh().get()
    val spaceHolder = tarantoolClient.metadata.getSpaceByName(SPACE_NAME)
    spaceHolder.isPresent should equal(true)
  }

  test("Load all space") {
    val rdd = TarantoolSpark.load[TarantoolTuple](sc, "_spark_test_space")
    rdd.count() > 0 should equal(true)
  }
}
