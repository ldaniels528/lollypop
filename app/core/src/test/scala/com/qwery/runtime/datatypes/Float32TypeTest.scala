package com.qwery.runtime.datatypes

import com.qwery.language.QweryUniverse
import com.qwery.runtime.Scope
import com.qwery.runtime.datatypes.DataTypeFunSpec.implicits.DataTypeConversion

class Float32TypeTest extends DataTypeFunSpec {
  implicit val ctx: QweryUniverse = QweryUniverse()
  implicit val scope: Scope = Scope()

  describe(Float32Type.getClass.getSimpleName) {

    it("should detect FloatType column types") {
      verifyType(value = Float.MaxValue, expectedType = Float32Type)
    }

    it("should encode/decode FloatType values") {
      verifyCodec(Float32Type, value = 12345.1f)
    }

    it("should conversions to FloatType") {
      assert("12345.1".convertTo(Float32Type) == 12345.1f)
    }

    it("should resolve 'Float'") {
      verifySpec(spec = "Float", expected = Float32Type)
    }

    it("should resolve 'Float[88]'") {
      verifySpec(spec = "Float[88]", expected = ArrayType(Float32Type, capacity = Some(88)))
    }

    it("should provide a SQL representation") {
      verifySQL("Float", Float32Type)
    }

  }

}
