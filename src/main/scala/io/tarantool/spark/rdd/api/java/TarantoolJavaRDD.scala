package io.tarantool.spark.rdd.api.java

import io.tarantool.driver.api.tuple.TarantoolTuple
import io.tarantool.spark.rdd.TarantoolRDD
import org.apache.spark.api.java.JavaRDD

class TarantoolJavaRDD(override val rdd: TarantoolRDD) extends JavaRDD[TarantoolTuple](rdd) {

}
