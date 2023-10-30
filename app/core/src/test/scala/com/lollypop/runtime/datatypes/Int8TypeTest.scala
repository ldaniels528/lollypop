package com.lollypop.runtime.datatypes

import com.lollypop.language.LollypopUniverse
import com.lollypop.runtime.Scope
import com.lollypop.runtime.datatypes.DataTypeFunSpec.implicits.DataTypeConversion

/**
 * Int8Type Tests
 */
class Int8TypeTest extends DataTypeFunSpec {
  implicit val ctx: LollypopUniverse = LollypopUniverse()
  implicit val scope: Scope = Scope()

  describe(Int8Type.getClass.getSimpleName) {

    it("should detect Int8Type column types") {
      verifyType(value = Byte.MaxValue, expectedType = Int8Type)
    }

    it("should encode/decode Int8Type values") {
      verifyCodec(Int8Type, value = 55.toByte)
    }

    it("should conversions to Int8Type") {
      assert("100".convertTo(Int8Type) == 100)
    }

    it("should resolve 'Byte'") {
      verifySpec(spec = "Byte", expected = Int8Type)
    }

    it("should resolve 'Byte[8]'") {
      verifySpec(spec = "Byte[8]", expected = ArrayType(Int8Type, capacity = Some(8)))
    }

    it("should provide a SQL representation") {
      verifySQL("Byte", Int8Type)
    }

  }

}
