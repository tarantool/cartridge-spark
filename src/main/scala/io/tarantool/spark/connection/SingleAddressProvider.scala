package io.tarantool.spark.connection

import io.tarantool.driver.{TarantoolServerAddress, TarantoolSingleAddressProvider}

class SingleAddressProvider(addr: TarantoolServerAddress) extends TarantoolSingleAddressProvider{

  override def getAddress: TarantoolServerAddress = addr
}
