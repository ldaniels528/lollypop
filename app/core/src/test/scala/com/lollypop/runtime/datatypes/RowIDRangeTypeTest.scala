package com.lollypop.runtime.datatypes

import com.lollypop.runtime.Scope
import lollypop.io.RowIDRange

class RowIDRangeTypeTest extends DataTypeFunSpec {
  implicit val scope: Scope = Scope()

  describe(classOf[RowIDRangeType].getSimpleName) {

    it("should compile: RowIDRange") {
      verifySpec(spec = "RowIDRange", expected = RowIDRangeType)
    }

    it("should compile: RowIDRange[20]") {
      verifySpec(spec = "RowIDRange[20]", expected = ArrayType(componentType = RowIDRangeType, capacity = Some(20)))
    }

    it("should decompile: RowIDRange") {
      verifySQL(sql = "RowIDRange", dataType = RowIDRangeType)
    }

    it("should decompile: RowIDRange[20]") {
      verifySQL(sql = "RowIDRange[20]", dataType = ArrayType(componentType = RowIDRangeType, capacity = Some(20)))
    }

    it("should convert an Array to a RowIDRange") {
      val array = (0L to 5000L).toArray
      assert(RowIDRangeType.convert(array) == RowIDRange(start = Some(0L), end = Some(5000L)))
    }

    it("should convert a Range to a RowIDRange") {
      val range = 0L to 5000L
      assert(RowIDRangeType.convert(range) == RowIDRange(start = Some(0L), end = Some(5000L)))
    }

    it("should convert a Seq to a RowIDRange") {
      val list = (0L to 5000L).toList
      assert(RowIDRangeType.convert(list) == RowIDRange(start = Some(0L), end = Some(5000L)))
    }

  }

}