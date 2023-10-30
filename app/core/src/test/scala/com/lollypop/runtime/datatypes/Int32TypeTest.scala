package com.lollypop.runtime.datatypes

import com.lollypop.language.LollypopUniverse
import com.lollypop.runtime.Scope
import com.lollypop.runtime.datatypes.DataTypeFunSpec.implicits.DataTypeConversion
import com.lollypop.runtime.errors.ArgumentMismatchError

/**
 * Int32Type Tests
 */
class Int32TypeTest extends DataTypeFunSpec {
  implicit val ctx: LollypopUniverse = LollypopUniverse()
  implicit val scope: Scope = Scope()

  describe(Int32Type.getClass.getSimpleName) {

    it("should detect Int32Type column types") {
      verifyType(value = Int.MaxValue, expectedType = Int32Type)
    }

    it("should encode/decode Int32Type values") {
      verifyCodec(Int32Type, value = 12345)
    }

    it("should conversions to Int32Type") {
      assert("12345".convertTo(Int32Type) == 12345)
    }

    it("should resolve 'Int'") {
      verifySpec(spec = "Int", expected = Int32Type)
    }

    it("should resolve 'Integer'") {
      verifySpec(spec = "Integer", expected = Int32Type)
    }

    it("should resolve 'Int[9]'") {
      verifySpec(spec = "Int[9]", expected = ArrayType(Int32Type, capacity = Some(9)))
    }

    it("should resolve 'Integer[9]'") {
      verifySpec(spec = "Integer[9]", expected = ArrayType(Int32Type, capacity = Some(9)))
    }

    it("should provide a SQL representation") {
      verifySQL("Int", Int32Type)
    }

    it("should fail if an incorrect number of arguments is specified") {
      assertThrows[ArgumentMismatchError]{
        Int32Type.construct(Seq(123, 456))
      }
    }

  }

}
