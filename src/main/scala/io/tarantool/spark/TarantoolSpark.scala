package io.tarantool.spark

import io.tarantool.spark.connection.{ReadOptions, TarantoolConfigBuilder}
import io.tarantool.spark.rdd.TarantoolRDD
import io.tarantool.spark.rdd.api.java.TarantoolJavaRDD
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext

import scala.reflect.ClassTag

object TarantoolSpark {

  /**
   * Load data from Tarantool into rdd
   *
   * @param sparkContext the sparkContext containing the Tarantool configuration
   * @param space        the space name to load data from
   */
  def load(sparkContext: SparkContext, space: String): TarantoolRDD = {
    load(sparkContext, TarantoolConfigBuilder.createReadOptions(space, sparkContext.getConf))
  }

  /**
   * Load data from Tarantool into rdd
   *
   * @param sparkContext the sparkContext containing the Tarantool configuration
   * @param options      the readOptions to load data from Tarantool
   */
  def load(sparkContext: SparkContext, options: ReadOptions): TarantoolRDD = {
    new TarantoolRDD(sparkContext, options)
  }

  /**
   * Load data from Tarantool into rdd
   *
   * @param sparkContext the sparkContext containing the Tarantool configuration
   * @param space        the space name to load data from
   *
   */
  def load(sparkContext: JavaSparkContext, space: String): TarantoolJavaRDD = {
    load(sparkContext, TarantoolConfigBuilder.createReadOptions(space, sparkContext.getConf))
  }

  /**
   * Load data from Tarantool into rdd
   *
   * @param sparkContext the sparkContext containing the Tarantool configuration
   * @param options      the readOptions to load data from Tarantool
   * @param space        the space name to load data from
   *
   */
  def load(sparkContext: JavaSparkContext, space: String, options: ReadOptions): TarantoolJavaRDD = {
    load(sparkContext, options.copy(space = space, options))
  }

  /**
   * Load data from Tarantool into rdd
   *
   * @param sparkContext the sparkContext containing the Tarantool configuration
   * @param options      the readOptions to load data from Tarantool
   */
  def load(sparkContext: JavaSparkContext, options: ReadOptions): TarantoolJavaRDD = {
    load(sparkContext.sc, options).toJavaRDD()
  }
}
