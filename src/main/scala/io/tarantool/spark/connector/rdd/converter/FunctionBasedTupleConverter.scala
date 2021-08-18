package io.tarantool.spark.connector.rdd.converter

import io.tarantool.driver.api.tuple.TarantoolTuple

import scala.reflect.ClassTag

/**
  * Converts tuples based on the passed transformation function
  *
  * @param converter tuple transformation function
  * @tparam R target entity type
  * @author Alexey Kuzin
  */
class FunctionBasedTupleConverter[R](converter: TarantoolTuple => R) extends TupleConverter[R] {

  override def convert(tuple: TarantoolTuple): R = converter(tuple)
}

object FunctionBasedTupleConverter {

  def apply[R](converter: TarantoolTuple => R): FunctionBasedTupleConverter[R] =
    new FunctionBasedTupleConverter[R](converter)

  def apply(): FunctionBasedTupleConverter[TarantoolTuple] =
    new FunctionBasedTupleConverter[TarantoolTuple](identity)
}

/**
  * Provides instances of {@link FunctionBasedTupleConverter}
  * @param converter tuple transformation function
  * @tparam R target entity type
  */
class FunctionBasedTupleConverterFactory[R](converter: TarantoolTuple => R)(
  @transient implicit val ct: ClassTag[R]
) extends TupleConverterFactory[R] {

  override def tupleConverter(): TupleConverter[R] = FunctionBasedTupleConverter(converter)

  /**
    * Get target entity class
    *
    * @return target entity class object
    */
  override def targetClass(): Class[R] = ct.runtimeClass.asInstanceOf[Class[R]]
}

object FunctionBasedTupleConverterFactory {

  def apply[R](converter: TarantoolTuple => R)(
    implicit ct: ClassTag[R]
  ): FunctionBasedTupleConverterFactory[R] = new FunctionBasedTupleConverterFactory[R](converter)

  def apply(): FunctionBasedTupleConverterFactory[TarantoolTuple] =
    new FunctionBasedTupleConverterFactory[TarantoolTuple](identity)
}
