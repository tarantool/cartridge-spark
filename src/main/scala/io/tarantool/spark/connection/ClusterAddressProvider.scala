package io.tarantool.spark.connection

import java.util

import io.tarantool.driver.{TarantoolClusterAddressProvider, TarantoolServerAddress}

class ClusterAddressProvider(addresses: Seq[TarantoolServerAddress])
  extends TarantoolClusterAddressProvider {

  override def getAddresses: util.Collection[TarantoolServerAddress] =
    scala.collection.JavaConverters.seqAsJavaListConverter(addresses).asJava
}
