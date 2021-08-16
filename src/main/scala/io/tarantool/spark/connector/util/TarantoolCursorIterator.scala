package io.tarantool.spark.connector.util

import io.tarantool.driver.api.cursor.TarantoolCursor
import io.tarantool.driver.protocol.Packable

import java.util.concurrent.atomic.AtomicBoolean

/**
  * Wraps the TarantoolCursor into standard Iterator
  *
  * @author Alexey Kuzin
  */
class TarantoolCursorIterator[T <: Packable](cursor: TarantoolCursor[T]) extends Iterator[T] {

  private val firstFetch: AtomicBoolean = new AtomicBoolean(false)
  private val hasNextVal: AtomicBoolean = new AtomicBoolean(false)

  override def next(): T = {
    var value = null.asInstanceOf[T]
    if (hasNext) {
      hasNextVal.synchronized {
        value = cursor.get()
        hasNextVal.set(cursor.next())
      }
    }
    value
  }

  override def hasNext: Boolean = {
    if (!firstFetch.get()) {
      firstFetch.synchronized {
        if (!firstFetch.get()) {
          hasNextVal.set(cursor.next())
          firstFetch.set(true)
        }
      }
    }
    hasNextVal.get
  }
}

/**
  * Companion object for TarantoolCursorIterator
  */
object TarantoolCursorIterator {

  def apply[T <: Packable](cursor: TarantoolCursor[T]): TarantoolCursorIterator[T] =
    new TarantoolCursorIterator(cursor)
}
