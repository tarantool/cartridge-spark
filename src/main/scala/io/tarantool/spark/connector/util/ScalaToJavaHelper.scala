package io.tarantool.spark.connector.util

import java.util.function.{Consumer => JConsumer}
import java.util.function.{Function => JFunction}
import java.util.function.{Supplier => JSupplier}
import scala.reflect.ClassTag

/**
  * Provides helper methods for using Scala classes in the Java code
  *
  * @author Alexey Kuzin
  */
object ScalaToJavaHelper {

  /**
    * Returns a `ClassTag` of a given runtime class
    */
  def getClassTag[T](clazz: Class[T]): ClassTag[T] = ClassTag(clazz)

  /**
    * Converts a Java {@link java.util.function.Function} to a Scala {@link Function1}
    */
  def toScalaFunction1[T1, R](f: JFunction[T1, R]): T1 => R = f.apply

  /**
    * Converts a Scala {@link Function1} to a Java {@link java.util.function.Function}
    */
  def toJavaFunction[T1, R](f: T1 => R): JFunction[T1, R] = new JFunction[T1, R] {
    override def apply(t: T1): R = f.apply(t)
  }

  /**
    * Converts a Scala {@link Function1} to a Java {@link java.util.function.Function}
    */
  def toJavaConsumer[T1, Void](f: T1 => Void): JConsumer[T1] = new JConsumer[T1] {
    override def accept(t: T1): Unit = f.apply(t)
  }

  /**
    * Converts a Scala "Supplier" to a Java {@link java.util.function.Supplier}
    */
  def toJavaSupplier[R](f: () => R): JSupplier[R] = new JSupplier[R] {
    override def get(): R = f.apply()
  }
}
