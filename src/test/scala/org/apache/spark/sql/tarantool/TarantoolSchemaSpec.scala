package org.apache.spark.sql.tarantool

import io.tarantool.driver.api.metadata.TestTarantoolMetadata
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

/**
  *
  *
  * @author Alexey Kuzin
  */
class TarantoolSchemaSpec extends AnyFlatSpec {

  behavior.of("TarantoolSchemaSpec")

  it should "generate valid struct type from a simple schema" in {
    val schema = TarantoolSchema(TestTarantoolMetadata())
    val expected = DataTypes.createStructType(
      Seq(
        DataTypes.createStructField("firstname", StringType, true),
        DataTypes.createStructField("middlename", StringType, true),
        DataTypes.createStructField("lastname", StringType, true),
        DataTypes.createStructField("id", StringType, true),
        DataTypes.createStructField("age", LongType, true),
        DataTypes.createStructField("salary", DecimalType.SYSTEM_DEFAULT, true),
        DataTypes.createStructField("discount", DoubleType, true),
        DataTypes.createStructField("favourite_constant", DoubleType, true),
        DataTypes.createStructField("married", BooleanType, true),
        DataTypes.createStructField("updated", LongType, true)
      ).toArray
    )
    val actual = schema.asStructType("testSpace")

    actual should contain theSameElementsAs expected
  }

}
