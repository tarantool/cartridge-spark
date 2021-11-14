package io.tarantool.spark.connector

import io.tarantool.driver.exceptions.TarantoolException

/**
  * Generic type for all module exceptions
  *
  * @author Alexey Kuzin
  */
trait TarantoolSparkException extends TarantoolException {}

object TarantoolSparkException {

  def apply(message: String): TarantoolSparkException =
    new TarantoolException(message) with TarantoolSparkException

  def apply(exception: Throwable): TarantoolSparkException =
    new TarantoolException(exception) with TarantoolSparkException

  def apply(message: String, exception: Throwable): TarantoolSparkException =
    new TarantoolException(message, exception) with TarantoolSparkException
}
