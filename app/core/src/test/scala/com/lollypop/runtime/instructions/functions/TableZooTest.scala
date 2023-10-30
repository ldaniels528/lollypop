package com.lollypop.runtime.instructions.functions

import com.lollypop.runtime.devices.{ByteArrayRowCollection, FileRowCollection, ModelRowCollection, ShardedRowCollection}
import com.lollypop.runtime.{LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class TableZooTest extends AnyFunSpec {

  describe(classOf[TableZoo].getSimpleName) {

    it("should produce a ByteArrayRowCollection") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|TableZoo(symbol: String(8), exchange: String(8), lastSale: Double, lastSaleTime: DateTime)
           |  .withMemorySupport(150)
           |  .build()
           |""".stripMargin)
      assert(Option(result).map(_.getClass) contains classOf[ByteArrayRowCollection])
    }

    it("should produce a FileRowCollection") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|TableZoo(symbol: String(8), exchange: String(8), lastSale: Double, lastSaleTime: DateTime)
           |  .build()
           |""".stripMargin)
      assert(Option(result).map(_.getClass) contains classOf[FileRowCollection])
    }

    it("should produce a ModelRowCollection") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|TableZoo(symbol: String(8), exchange: String(8), lastSale: Double, lastSaleTime: DateTime)
           |  .withMemorySupport(0)
           |  .build()
           |""".stripMargin)
      assert(Option(result).map(_.getClass) contains classOf[ModelRowCollection])
    }

    it("should produce a ShardedRowCollection") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|TableZoo(symbol: String(8), exchange: String(8), lastSale: Double, lastSaleTime: DateTime)
           |  .withShardSupport(5000, TableZoo(symbol: String(8), exchange: String(8), lastSale: Double, lastSaleTime: DateTime))
           |  .build()
           |""".stripMargin)
      assert(Option(result).map(_.getClass) contains classOf[ShardedRowCollection])
    }

  }

}
