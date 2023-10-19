package com.qwery.runtime.datatypes

import com.qwery.language.QweryUniverse
import com.qwery.runtime.Scope
import com.qwery.runtime.datatypes.DataTypeFunSpec.implicits.DataTypeConversion

/**
 * BooleanType Tests
 */
class BooleanTypeTest extends DataTypeFunSpec {
  implicit val ctx: QweryUniverse = QweryUniverse()
  implicit val scope: Scope = Scope()

  describe(BooleanType.getClass.getSimpleName) {

    it("should detect BooleanType column types") {
      verifyType(value = true, expectedType = BooleanType)
      verifyType(value = false, expectedType = BooleanType)
    }

    it("should encode/decode BooleanType values") {
      verifyCodec(BooleanType, value = true)
    }

    it("should support conversions to BooleanType: 'true' | 'false'") {
      assert("true".convertTo(BooleanType) == true)
      assert("false".convertTo(BooleanType) == false)
    }

    it("should support conversions to BooleanType: true | false") {
      assert(true.convertTo(BooleanType) == true)
      assert(false.convertTo(BooleanType) == false)
    }

    it("should support conversions to BooleanType: 1 | 0") {
      assert(1.convertTo(BooleanType) == true)
      assert(0.convertTo(BooleanType) == false)
    }

    it("should resolve 'Boolean'") {
      verifySpec(spec = "Boolean", expected = BooleanType)
    }

    it("should resolve 'Boolean[6]'") {
      verifySpec(spec = "Boolean[6]", expected = ArrayType(BooleanType, capacity = Some(6)))
    }

    it("should provide a SQL representation") {
      verifySQL("Boolean", BooleanType)
    }
  }

}
