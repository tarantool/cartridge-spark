package io.tarantool.spark.integration

import io.tarantool.driver.api.TarantoolClient
import io.tarantool.spark.TarantoolSpark
import io.tarantool.spark.connection.{TarantoolConfigBuilder, TarantoolConnection}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TarantoolSparkReadStandaloneClientTest extends AnyFlatSpec with Matchers with SharedSparkContextStandaloneClient {

  private val SPACE_NAME: String = "_spark_test_space"
  private var tarantoolClient: TarantoolClient = _

  it should "Create connection from spark config" in {
    val tarantoolConnection = TarantoolConnection()
    tarantoolClient = tarantoolConnection.client(TarantoolConfigBuilder.createReadOptions(SPACE_NAME, sc.getConf))
    tarantoolClient.metadata.refresh().get()
    val spaceHolder = tarantoolClient.metadata.getSpaceByName(SPACE_NAME)
    spaceHolder.isPresent should equal(true)
  }

  it should "Load space in RDD" in {
    val rdd = TarantoolSpark.load(sc, SPACE_NAME)
    rdd.count() should equal(100)
  }
}
