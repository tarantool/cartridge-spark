package org.apache.spark.sql.tarantool

import io.tarantool.driver.api.metadata.TestTarantoolMetadata
import io.tarantool.driver.api.tuple.TarantoolTuple
import io.tarantool.driver.api.{TarantoolClient, TarantoolResult}
import io.tarantool.spark.connector.config.TarantoolConfig
import io.tarantool.spark.connector.connection.TarantoolConnection
import org.apache.spark.sql.types._
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

/**
  *
  *
  * @author Alexey Kuzin
  */
class TarantoolSchemaSpec extends AnyFlatSpec with MockFactory {

  behavior.of("TarantoolSchemaSpec")

  it should "generate valid struct type from a simple schema" in {
    val mockClient = stub[TarantoolClient[TarantoolTuple, TarantoolResult[TarantoolTuple]]]
    (mockClient.metadata _).when().returns(TestTarantoolMetadata())

    case class TarantoolConnectionMock(tarantoolConfig: TarantoolConfig)
        extends TarantoolConnection[TarantoolTuple, TarantoolResult[TarantoolTuple]](
          tarantoolConfig,
          (_, _) => mockClient
        )

    val mockTarantoolConnection = stub[TarantoolConnectionMock]
    (mockTarantoolConnection.client _).when().returns(mockClient)

    val mockSchema = TarantoolSchema(mockTarantoolConnection)
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
    val actual = mockSchema.asStructType("testSpace")

    actual should contain theSameElementsAs expected
  }

}
