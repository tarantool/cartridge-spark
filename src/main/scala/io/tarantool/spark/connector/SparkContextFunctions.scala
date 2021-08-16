package io.tarantool.spark.connector

import io.tarantool.driver.api.conditions.Conditions
import io.tarantool.driver.api.tuple.TarantoolTuple
import io.tarantool.spark.connector.config.ReadConfig
import org.apache.spark.SparkContext

import scala.reflect.ClassTag

/**
  * Spark API for Tarantool. Provides Tarantool-specific methods on [[org.apache.spark.SparkContext SparkContext]]
  *
  * @author Alexey Kuzin
  */
class SparkContextFunctions(@transient val sc: SparkContext) extends Serializable {

  /**
    * Load data from Tarantool space as `TarantoolRDD`, filtering it with conditions.
    *
    * This method is made available on [[org.apache.spark.SparkContext SparkContext]] by importing
    * io.tarantool.spark._
    *
    * Example:
    * {{{
    *   //TODO: populate Tarantool space
    * }}}
    * {{{
    *   //TODO: load from space
    * }}}
    *
    * @param space        space name
    * @param conditions   filtering conditions
    * @return {@link TarantoolRDD} with tuples from the space
    */
  def tarantoolSpace[R](
    space: String,
    conditions: Conditions = Conditions.any(),
    tupleConverter: TarantoolTuple => R = (value: TarantoolTuple) => value.asInstanceOf[R]
  )(
    implicit
    ctr: ClassTag[R],
    sparkContext: SparkContext = sc,
    readConfig: ReadConfig = ReadConfig()
  ) = rdd.TarantoolRDD(sparkContext, space, conditions, readConfig, tupleConverter)

}
