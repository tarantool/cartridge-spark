package io.tarantool.spark.sql

import io.tarantool.driver.metadata.TestSpaceMetadata
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
    val spaceMetadata = TestSpaceMetadata()
    val expected = DataTypes.createStructType(
      Seq(
        DataTypes.createStructField("firstname", StringType, true),
        DataTypes.createStructField("middlename", StringType, true),
        DataTypes.createStructField("lastname", StringType, true),
        DataTypes.createStructField("id", StringType, true),
        DataTypes.createStructField("age", LongType, true),
        DataTypes.createStructField("salary", DataTypes.createDecimalType(), true),
        DataTypes.createStructField("discount", DoubleType, true),
        DataTypes.createStructField("favourite_constant", DoubleType, true),
        DataTypes.createStructField("married", BooleanType, true),
        DataTypes.createStructField("updated", LongType, true)
      ).toArray
    )
    val actual = TarantoolSchema.asStructType(spaceMetadata)

    actual should contain theSameElementsAs expected
  }

}
