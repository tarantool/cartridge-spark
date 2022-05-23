package io.tarantool.spark.connector.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class StringUtilsSpec extends AnyFlatSpec with Matchers {
  "camelToSnake" should "process right camel case + all upper case + mixed" in {
    StringUtils.camelToSnake("COLUMN") shouldBe "column"
    StringUtils.camelToSnake("someColumnNameRespectingCamel") shouldBe "some_column_name_respecting_camel"
    StringUtils.camelToSnake("columnWITHSomeALLUppercaseWORDS") shouldBe "column_with_some_all_uppercase_words"
    StringUtils.camelToSnake("_column") shouldBe "_column"
    StringUtils.camelToSnake("column_") shouldBe "column_"
    StringUtils.camelToSnake("Column") shouldBe "column"
    StringUtils.camelToSnake("Column_S") shouldBe "column_s"
    StringUtils.camelToSnake("Column_SV") shouldBe "column_sv"
    StringUtils.camelToSnake("Column_String") shouldBe "column_string"
    StringUtils.camelToSnake("_columnS") shouldBe "_column_s"
    StringUtils.camelToSnake("column1234") shouldBe "column1234"
    StringUtils.camelToSnake("column1234string") shouldBe "column1234string"
  }
}
