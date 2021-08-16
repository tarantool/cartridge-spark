package io.tarantool.spark.connector.config

import io.tarantool.driver.{TarantoolClusterAddressProvider, TarantoolServerAddress}

import java.util

class StaticClusterAddressProvider(addresses: Seq[TarantoolServerAddress])
    extends TarantoolClusterAddressProvider {

  override def getAddresses: util.Collection[TarantoolServerAddress] =
    scala.collection.JavaConverters.seqAsJavaListConverter(addresses).asJava
}
