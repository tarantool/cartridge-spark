package io.tarantool.spark.partition

import io.tarantool.driver.api.TarantoolSelectOptions
import io.tarantool.spark.connection.ReadOptions
import org.apache.spark.Partition

case class TarantoolPartition(index: Int,
                              options: ReadOptions,
                              selectOptions: TarantoolSelectOptions)
    extends Partition {}
