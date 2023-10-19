package com.qwery.runtime.datatypes

import com.qwery.language.QweryUniverse
import com.qwery.runtime.Scope
import com.qwery.runtime.datatypes.DataTypeFunSpec.implicits.DataTypeConversion

/**
 * Int64Type Tests
 */
class Int64TypeTest extends DataTypeFunSpec {
  implicit val ctx: QweryUniverse = QweryUniverse()
  implicit val scope: Scope = Scope()

  describe(Int64Type.getClass.getSimpleName) {

    it("should detect Int64Type column types") {
      verifyType(value = Long.MaxValue, expectedType = Int64Type)
    }

    it("should encode/decode Int64Type values") {
      verifyCodec(Int64Type, value = 0xDEADBEEFL)
    }

    it("should conversions to Int64Type") {
      assert("12345".convertTo(Int64Type) == 12345L)
    }

    it("should resolve 'Long'") {
      verifySpec(spec = "Long", expected = Int64Type)
    }

    it("should resolve 'Long[9]'") {
      verifySpec(spec = "Long[9]", expected = ArrayType(Int64Type, capacity = Some(9)))
    }

    it("should provide a SQL representation") {
      verifySQL("Long", Int64Type)
    }

  }

}
