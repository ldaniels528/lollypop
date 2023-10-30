package com.lollypop.runtime.datatypes

import com.lollypop.language.LollypopUniverse
import com.lollypop.runtime.Scope
import com.lollypop.runtime.datatypes.DataTypeFunSpec.implicits.DataTypeConversion

class Float64TypeTest extends DataTypeFunSpec {
  implicit val ctx: LollypopUniverse = LollypopUniverse()
  implicit val scope: Scope = Scope()

  describe(Float64Type.getClass.getSimpleName) {

    it("should detect Float64Type column types") {
      verifyType(value = Double.MaxValue, expectedType = Float64Type)
    }

    it("should encode/decode Float64Type values") {
      verifyCodec(Float64Type, value = 12345.6789d)
    }

    it("should conversions to Float64Type") {
      assert("1234567.890".convertTo(Float64Type) == 1234567.890)
    }

    it("should resolve 'Double'") {
      verifySpec(spec = "Double", expected = Float64Type)
    }

    it("should resolve 'Double[9]' and 'real[9]'") {
      verifySpec(spec = "Double[9]", expected = ArrayType(Float64Type, capacity = Some(9)))
    }

    it("should provide a SQL representation") {
      verifySQL("Double", Float64Type)
    }

  }

}
