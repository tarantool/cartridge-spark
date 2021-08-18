package io.tarantool.spark.connector

import io.tarantool.driver.api.conditions.Conditions
import io.tarantool.spark.connector.config.ReadConfig
import io.tarantool.spark.connector.rdd.TarantoolRDD
import io.tarantool.spark.connector.rdd.converter.TupleConverterFactory
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
    * <p>This method is made available on [[org.apache.spark.SparkContext SparkContext]] by importing
    * io.tarantool.spark._</p>
    *
    * <p>Example:
    * <pre>
    * local crud = require('crud')
    *
    * crud.insert('test_space', {1, nil, 'a1', 'Don Quixote', 'Miguel de Cervantes', 1605})
    * crud.insert('test_space', {2, nil, 'a2', 'The Great Gatsby', 'F. Scott Fitzgerald', 1925})
    * crud.insert('test_space', {3, nil, 'a3', 'War and Peace', 'Leo Tolstoy', 1869})
    * ...
    *
    * val rdd = sc.tarantoolSpace("test_space", Conditions.indexGreaterThan("id", Collections.singletonList(1)));
    * rdd.first().getInteger("id"); // 1
    * rdd.first().getString("author"); // "Miguel de Cervantes"
    * </pre>
    * </p>
    *
    * @param space        space name
    * @param conditions   filtering conditions
    * @return {@link TarantoolRDD} with tuples from the space
    */
  def tarantoolSpace[R](
    space: String,
    conditions: Conditions
  )(
    implicit
    ct: ClassTag[R],
    sparkContext: SparkContext = sc,
    readConfig: ReadConfig = ReadConfig(),
    tupleConverterFactory: TupleConverterFactory[R]
  ): TarantoolRDD[R] =
    TarantoolRDD(
      sparkContext,
      space,
      conditions,
      readConfig,
      tupleConverterFactory.tupleConverter()
    )
}
