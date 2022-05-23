package io.tarantool.spark.connector.util

import scala.annotation.tailrec

/**
  * Provides helper methods for transforming String instances
  *
  * @author Alexey Kuzin
  */
object StringUtils {

  /**
    * Converts from camelCase to snake_case
    * e.g.: camelCase => camel_case
    *
    * Borrowed from https://gist.github.com/sidharthkuruvila/3154845?permalink_comment_id=2622928#gistcomment-2622928
    *
    * @param name the camelCase name to convert
    * @return snake_case version of the string passed
    */
  def camelToSnake(name: String): String = {
    @tailrec
    def go(accDone: List[Char], acc: List[Char]): List[Char] = acc match {
      case Nil => accDone
      case a :: b :: c :: tail if a.isUpper && b.isUpper && c.isLower =>
        go(accDone ++ List(a, '_', b, c), tail)
      case a :: b :: tail if a.isLower && b.isUpper => go(accDone ++ List(a, '_', b), tail)
      case a :: tail                                => go(accDone :+ a, tail)
    }
    go(Nil, name.toList).mkString.toLowerCase
  }
}
