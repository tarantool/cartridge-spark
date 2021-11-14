package io.tarantool.spark.connector

import io.tarantool.driver.exceptions.TarantoolException

/**
  * Generic type for all module exceptions
  *
  * @author Alexey Kuzin
  */
case class TarantoolSparkException(message: String) extends TarantoolException(message) {}

object TarantoolSparkException {

  def TarantoolSparkException(message: String): TarantoolSparkException =
    new TarantoolSparkException(message)
}
