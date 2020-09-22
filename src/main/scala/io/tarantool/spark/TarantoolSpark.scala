package io.tarantool.spark

import io.tarantool.spark.connection.{ReadOptions, TarantoolConfigBuilder}
import io.tarantool.spark.rdd.TarantoolRDD
import org.apache.spark.SparkContext

import scala.reflect.ClassTag

object TarantoolSpark {

  /**
   * Load data from Tarantool into rdd
   *
   * @param sc    the sparkContext containing the Tarantool configuration
   * @param space the space name to load data from
   */
  def load[T: ClassTag](sc: SparkContext, space: String): TarantoolRDD[T] = {
    load(sc, TarantoolConfigBuilder.createReadOptions(space, sc.getConf))
  }

  /**
   * Load data from Tarantool into rdd
   *
   * @param sc      the sparkContext containing the Tarantool configuration
   * @param options the readOptions to load data from Tarantool
   */
  def load[T: ClassTag](sc: SparkContext, options: ReadOptions): TarantoolRDD[T] = {
    new TarantoolRDD[T](sc, options)
  }
}
