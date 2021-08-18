package io.tarantool.spark.connector.rdd.converter

import io.tarantool.driver.api.tuple.TarantoolTuple

import scala.annotation.implicitNotFound
import scala.reflect.runtime.universe.TypeTag

/**
  * Converts {@link TarantoolTuple}s to instances of the given type
  *
  * @tparam R target entity type
  * @author Alexey Kuzin
  */
trait TupleConverter[R] extends Serializable {

  /**
    * Takes a Tarantool tuple and converts it into an instance of type R
    * @param tuple the raw tuple
    * @return new instance of type {@code R}
    */
  def convert(tuple: TarantoolTuple): R
}

/**
  * Provides intances of {@link TupleConverter}
  * @tparam R target entity type
  */
@implicitNotFound("No TupleConverterFactory can be found for this type")
trait TupleConverterFactory[R] {

  /**
    * Create a new {@link TupleConverter} instance
    * @return new tuple converter instance
    */
  def tupleConverter(): TupleConverter[R]

  /**
    * Get target entity class
    * @return target entity class object
    */
  def targetClass(): Class[R]
}

trait LowPriorityTupleConverterFactoryImplicits {

  implicit def functionBasedTupleConverterFactory[R <: TarantoolTuple](
    implicit
    tt: TypeTag[R]
  ): TupleConverterFactory[TarantoolTuple] =
    FunctionBasedTupleConverterFactory()
}

object TupleConverterFactory extends LowPriorityTupleConverterFactoryImplicits {}
