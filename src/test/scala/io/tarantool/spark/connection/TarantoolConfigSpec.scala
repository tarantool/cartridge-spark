package io.tarantool.spark.connection

import io.tarantool.driver.TarantoolServerAddress
import org.apache.spark.SparkConf
import org.scalatest.{FlatSpec, Matchers}

class TarantoolConfigSpec extends FlatSpec with Matchers {

  it should "apply default settings" in {
    val sparkConf = new SparkConf()

    val tConf: ReadOptions = TarantoolConfigBuilder.createReadOptions("space_1", sparkConf)
    tConf.space should equal("space_1")
    tConf.hosts should equal(Array(new TarantoolServerAddress("127.0.0.1:3301")))
    tConf.credentials should equal(None)
    tConf.timeouts should equal(Timeouts(None, None, None))
    tConf.useProxyClient should equal(false)
    tConf.clusterDiscoveryConfig should equal(None)
  }

  it should "apply default settings with timeouts" in {
    val sparkConf = new SparkConf()
      .set("tarantool.connectTimeout", "10")
      .set("tarantool.readTimeout", "20")
      .set("tarantool.requestTimeout", "30")
    val tConf: ReadOptions = TarantoolConfigBuilder.createReadOptions("space_1", sparkConf)
    tConf.space should equal("space_1")
    tConf.hosts should equal(Array(new TarantoolServerAddress("127.0.0.1:3301")))
    tConf.credentials should equal(None)
    tConf.timeouts should equal(Timeouts(Some(10), Some(20), Some(30)))
    tConf.useProxyClient should equal(false)
    tConf.clusterDiscoveryConfig should equal(None)
  }

  it should "apply cluster settings" in {
    val sparkConf = new SparkConf()
      .set("tarantool.useClusterDiscovery", "1")
      .set("tarantool.useProxyClient", "true")

    val tConf: ReadOptions = TarantoolConfigBuilder.createReadOptions("space_2", sparkConf)
    tConf.space should equal("space_2")
    tConf.hosts should equal(Array(new TarantoolServerAddress("127.0.0.1:3301")))
    tConf.credentials should equal(None)
    tConf.timeouts should equal(Timeouts(None, None, None))
    tConf.useProxyClient should equal(true)
    tConf.clusterDiscoveryConfig should equal(None)
  }

  it should "apply cluster cluster with HttpDiscovery settings" in {
    val sparkConf = new SparkConf()
      .set("tarantool.useClusterDiscovery", "1")

      .set("tarantool.discoveryProvider", "http")
      .set("tarantool.discoverConnectTimeout", "50")
      .set("tarantool.discoveryReadTimeout", "60")
      .set("tarantool.discoveryDelay", "70")
      .set("tarantool.discoveryHttpUrl", "https://www.tarantool.io/en/doc/latest/book/cartridge/")

    val tConf: ReadOptions = TarantoolConfigBuilder.createReadOptions("space_2", sparkConf)
    tConf.space should equal("space_2")
    tConf.hosts should equal(Array(new TarantoolServerAddress("127.0.0.1:3301")))
    tConf.credentials should equal(None)
    tConf.timeouts should equal(Timeouts(None, None, None))

    val discoveryConfig = ClusterDiscoveryConfig(
      provider = TarantoolDefaults.DISCOVERY_PROVIDER_HTTP,
      timeouts = ClusterDiscoveryTimeouts(Some(50), Some(60), Some(70)),
      httpDiscoveryConfig = Some(ClusterHttpDiscoveryConfig(
        url = "https://www.tarantool.io/en/doc/latest/book/cartridge/")
      ),
      binaryDiscoveryConfig = None
    )

    tConf.clusterDiscoveryConfig should equal(Some(discoveryConfig))
  }

  it should "apply cluster cluster with BinaryDiscovery settings" in {
    val sparkConf = new SparkConf()
      .set("tarantool.useClusterDiscovery", "true")

      .set("tarantool.discoveryProvider", "binary")
      .set("tarantool.discoverConnectTimeout", "80")
      .set("tarantool.discoveryReadTimeout", "90")
      .set("tarantool.discoveryDelay", "100")
      .set("tarantool.discoveryBinaryEntryFunction", "get_cluster_routers")
      .set("tarantool.discoveryBinaryHost", "127.0.1.2:5555")

    val tConf: ReadOptions = TarantoolConfigBuilder.createReadOptions("space_2", sparkConf)
    tConf.space should equal("space_2")
    tConf.hosts should equal(Array(new TarantoolServerAddress("127.0.0.1:3301")))
    tConf.credentials should equal(None)
    tConf.timeouts should equal(Timeouts(None, None, None))

    val discoveryConfig = ClusterDiscoveryConfig(
      provider = TarantoolDefaults.DISCOVERY_PROVIDER_BINARY,
      timeouts = ClusterDiscoveryTimeouts(Some(80), Some(90), Some(100)),
      httpDiscoveryConfig = None,
      binaryDiscoveryConfig = Some(ClusterBinaryDiscoveryConfig(
        entryFunction = "get_cluster_routers",
        address = new TarantoolServerAddress("127.0.1.2:5555")
      ))
    )
    tConf.clusterDiscoveryConfig should equal(Some(discoveryConfig))
  }
}
