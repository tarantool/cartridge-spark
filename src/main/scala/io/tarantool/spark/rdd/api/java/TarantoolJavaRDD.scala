package io.tarantool.spark.rdd.api.java

import io.tarantool.spark.rdd.TarantoolRDD
import org.apache.spark.api.java.JavaRDD

import scala.reflect.ClassTag

class TarantoolJavaRDD[T](override val rdd: TarantoolRDD[T])(implicit override val classTag: ClassTag[T])
  extends JavaRDD[T](rdd)(classTag) {

}
