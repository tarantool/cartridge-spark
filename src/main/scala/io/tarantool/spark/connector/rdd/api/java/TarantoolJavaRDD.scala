package io.tarantool.spark.connector.rdd.api.java

import io.tarantool.spark.connector.rdd.TarantoolRDD
import org.apache.spark.api.java.JavaRDD

import scala.reflect.ClassTag

class TarantoolJavaRDD[R](override val rdd: TarantoolRDD[R])(
  override implicit val classTag: ClassTag[R]
) extends JavaRDD[R](rdd) {}
