package io.tarantool.spark.connector.integration

import org.scalatest.BeforeAndAfterEach
import org.scalatest.Suite

/**
  * @author Alexey Kuzin
  */
trait TarantoolSparkClusterTestSuite extends BeforeAndAfterEach { this: Suite =>

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    SharedSparkContext.setupSpark()
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    SharedSparkContext.teardownSpark()
  }
}
