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
  def load[T: ClassTag](sparkContext: SparkContext, space: String): TarantoolRDD[T] = {
    load(sparkContext, TarantoolConfigBuilder.createReadOptions(space, sparkContext.getConf))
  }

  /**
   * Load data from Tarantool into rdd
   *
   * @param sparkContext the sparkContext containing the Tarantool configuration
   * @param options      the readOptions to load data from Tarantool
   */
  def load[T: ClassTag](sparkContext: SparkContext, options: ReadOptions): TarantoolRDD[T] = {
    new TarantoolRDD[T](sparkContext, options)
  }

  /**
   * Load data from Tarantool into rdd
   *
   * @param sparkContext the sparkContext containing the Tarantool configuration
   * @param space        the space name to load data from
   * @param clazz        the class of result type
   *
   */
  def load[T](sparkContext: JavaSparkContext, space: String, clazz: Class[T]): TarantoolJavaRDD[T] = {
    load(sparkContext, TarantoolConfigBuilder.createReadOptions(space, sparkContext.getConf), clazz)
  }

  /**
   * Load data from Tarantool into rdd
   *
   * @param sparkContext the sparkContext containing the Tarantool configuration
   * @param options      the readOptions to load data from Tarantool
   * @param space        the space name to load data from
   * @param clazz        the class of result type
   *
   */
  def load[T](sparkContext: JavaSparkContext, space: String, options: ReadOptions, clazz: Class[T]): TarantoolJavaRDD[T] = {
    load(sparkContext, options.copy(space = space, options), clazz)
  }

  /**
   * Load data from Tarantool into rdd
   *
   * @param sparkContext the sparkContext containing the Tarantool configuration
   * @param options      the readOptions to load data from Tarantool
   * @param clazz        the class of result type
   */
  def load[T](sparkContext: JavaSparkContext, options: ReadOptions, clazz: Class[T]): TarantoolJavaRDD[T] = {
    implicit val classtag: ClassTag[T] = ClassTag(clazz)
    load(sparkContext.sc, options).toJavaRDD()
  }
}
