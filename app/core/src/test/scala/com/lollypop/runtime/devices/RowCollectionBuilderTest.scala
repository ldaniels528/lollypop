package com.lollypop.runtime.devices

import com.lollypop.runtime.datatypes.{DateTimeType, Float32Type, StringType}
import org.scalatest.funspec.AnyFunSpec

class RowCollectionBuilderTest extends AnyFunSpec {
  private val columns = Seq(
    TableColumn(name = "symbol", `type` = StringType(6)),
    TableColumn(name = "exchange", `type` = StringType(6)),
    TableColumn(name = "lastSale", `type` = Float32Type),
    TableColumn(name = "tradeDate", `type` = DateTimeType)
  )

  describe(classOf[RowCollectionBuilder].getSimpleName) {

    it("should provide a ByteArrayRowCollection") {
      val coll = RowCollectionBuilder()
        .withColumns(columns)
        .withMemorySupport(150)
        .build
      assert(coll.isInstanceOf[ByteArrayRowCollection])
    }

    it("should provide a ModelRowCollection") {
      val coll = RowCollectionBuilder()
        .withColumns(columns)
        .withMemorySupport()
        .build
      assert(coll.isInstanceOf[ModelRowCollection])
    }

    it("should provide a FileRowCollection") {
      val coll = RowCollectionBuilder()
        .withColumns(columns)
        .build
      assert(coll.isInstanceOf[FileRowCollection])
    }

    it("should provide a ShardedRowCollection (RAF)") {
      val coll = RowCollectionBuilder()
        .withColumns(columns)
        .withSharding(shardSize = 5000)
        .build
      assert(coll.isInstanceOf[ShardedRowCollection])
      assert(coll.asInstanceOf[ShardedRowCollection].shardBuilder.build.isInstanceOf[FileRowCollection])
    }

    it("should provide a ShardedRowCollection (Memory)") {
      val coll = RowCollectionBuilder()
        .withColumns(columns)
        .withSharding(shardSize = 5000, builder = _.withMemorySupport())
        .build
      assert(coll.isInstanceOf[ShardedRowCollection])
      assert(coll.asInstanceOf[ShardedRowCollection].shardBuilder.build.isInstanceOf[ModelRowCollection])
    }

  }

}
