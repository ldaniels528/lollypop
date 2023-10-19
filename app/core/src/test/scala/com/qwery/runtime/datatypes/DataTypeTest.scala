package com.qwery.runtime.datatypes

import com.qwery.language.QweryUniverse
import com.qwery.language.models.Expression.implicits.LifestyleExpressionsAny
import com.qwery.language.models.Operation._
import com.qwery.language.models.{Column, ColumnType}
import com.qwery.runtime.datatypes.Inferences.InstructionTyping
import com.qwery.runtime.devices.RowCollectionZoo.ProductClassToTableType
import com.qwery.runtime.Scope
import qwery.io.IOCost

/**
 * DataType Tests
 */
class DataTypeTest extends DataTypeFunSpec {
  implicit val ctx: QweryUniverse = QweryUniverse()
  implicit val scope: Scope = Scope()

  describe(classOf[DataType].getSimpleName) {

    it("should convert a column type (Any) to a data type") {
      val column = Column(name = "value", `type` = ColumnType("Any"))
      assert(DataType(column.`type`) == AnyType)
    }

    it("should infer (5 + 7) as Int32Type") {
      val expr = 5.v + 7.v
      assert(expr.returnType == Int32Type)
    }

    it("should infer (5L - 7) as Int64Type") {
      val expr = 5.v - 7L.v
      assert(expr.returnType == Int64Type)
    }

    it("should infer (5L * 7.0) as Float64Type") {
      val expr = 5L.v * 7.0.v
      assert(expr.returnType == Float64Type)
    }

    it("should infer (5.0 / 7) as Float64Type") {
      val expr = 5.0.v / 7.v
      assert(expr.returnType == Float64Type)
    }

    it("should infer (5.0 + 7.0) as Float64Type") {
      val expr = 5.0.v + 7.0.v
      assert(expr.returnType == Float64Type)
    }

    it("should infer ('Hello ' + 7) as StringType") {
      val expr = "Hello ".v + 7.v
      assert(expr.returnType.name == StringType.name)
    }

    it("should infer ('Hello ' + 7.0) as StringType") {
      val expr = "Hello ".v + 7.0.v
      assert(expr.returnType.name == StringType.name)
    }

    it("should infer an IOCost result as TableType") {
      val expr = IOCost()
      assert(expr.returnType == classOf[IOCost].toTableType)
    }

  }

}


