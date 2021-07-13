package io.tarantool.spark

import org.apache.spark.SparkContext

/**
  * Tarantool connector for Apache Spark.
  *
  * Call [[io.tarantool.spark.connector.SparkContextFunctions#tarantoolSpace tarantoolSpace]] method on
  * the [[org.apache.spark.SparkContext SparkContext]] object
  * to create a [[io.tarantool.spark.connector.rdd.TarantoolRDD TarantoolRDD]] exposing
  * Tarantool space as a Spark RDD.
  *
  * Example:
  *
  * Execute the following on a Cartridge router node (the tarantool/crud module must be installed):
  * {{{
  *  local crud = require('crud')
  *
  *  crud.insert('test_space', {1, nil, 'a1', 'Don Quixote', 'Miguel de Cervantes', 1605})
  *  crud.insert('test_space', {2, nil, 'a2', 'The Great Gatsby', 'F. Scott Fitzgerald', 1925})
  *  crud.insert('test_space', {3, nil, 'a3', 'War and Peace', 'Leo Tolstoy', 1869})
  * }}}
  *
  * Write the following in your Java client code:
  * {{{
  *   import io.tarantool.spark.connector._
  *
  *   val sparkMasterHost = "127.0.0.1"
  *   val tarantoolRouterAddress = "127.0.0.1:3301"
  *   val space = "test_space"
  *
  *   // Populate the Spark config with the address of a Cartridge router node and credentials:
  *   val conf = new SparkConf(true)
  *   conf.set ("tarantool.username", "admin")
  *   conf.set ("tarantool.password", "testapp-cluster-cookie")
  *   conf.set ("tarantool.hosts", tarantoolRouterAddress)
  *
  *   // Connect to the Spark cluster:
  *   val sc = new SparkContext("spark://" + sparkMasterHost + ":7077", "example", conf)
  *
  *   // Read the space and print its contents:
  *   val rdd = sc.tarantoolSpace(space)
  *   rdd.toArray().foreach(println)
  *
  *   sc.stop()
  * }}}
  */
package object connector {

  implicit def toSparkContextFunctions(sc: SparkContext): SparkContextFunctions =
    new SparkContextFunctions(sc)
}
