package io.tarantool.spark.connector.rdd

/**
  * Baic trait for RDD implementations.
  *
  * @author Alexey Kuzin
  */
trait TarantoolBaseRDD {

  /**
    * Tarantool space name.
    */
  val space: String
}
