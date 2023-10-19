package com.qwery.runtime.datatypes

import com.qwery.language.QweryUniverse
import com.qwery.runtime.Scope
import com.qwery.runtime.datatypes.DataTypeFunSpec.implicits.DataTypeConversion

/**
 * Int16Type Tests
 */
class Int16TypeTest extends DataTypeFunSpec {
  implicit val ctx: QweryUniverse = QweryUniverse()
  implicit val scope: Scope = Scope()

  describe(Int16Type.getClass.getSimpleName) {

    it("should detect Int16Type column types") {
      verifyType(value = Short.MaxValue, expectedType = Int16Type)
    }

    it("should encode/decode Int16Type values") {
      verifyCodec(Int16Type, value = 12345.toShort)
    }

    it("should conversions to Int16Type") {
      assert("12345".convertTo(Int16Type) == 12345)
    }

    it("should resolve 'Short', 'smallInt'") {
      verifySpec(spec = "Short", expected = Int16Type)
    }

    it("should resolve 'Short[33]' and 'smallInt[33]'") {
      verifySpec(spec = "Short[33]", expected = ArrayType(Int16Type, capacity = Some(33)))
    }

    it("should provide a SQL representation") {
      verifySQL("Short", Int16Type)
    }

  }

}
