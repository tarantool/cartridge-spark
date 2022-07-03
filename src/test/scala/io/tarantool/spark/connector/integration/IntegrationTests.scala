package io.tarantool.spark.connector.integration

import org.apache.spark.sql.SQLImplicits
import org.scalatest._

class IntegrationTests
    extends Suites(
      new TarantoolConnectionSpec,
      new TarantoolSparkReadClusterTest,
      new TarantoolSparkWriteClusterTest
    )
    with BeforeAndAfterAll {
  protected lazy val sqlImplicits: SQLImplicits = SharedSparkContext.spark.implicits

  override def beforeAll(): Unit = {
    super.beforeAll()
    SharedSparkContext.setup()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    SharedSparkContext.teardown()
  }
}
