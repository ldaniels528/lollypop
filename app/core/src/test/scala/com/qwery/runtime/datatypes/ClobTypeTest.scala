package com.qwery.runtime.datatypes

import com.qwery.language.QweryUniverse
import com.qwery.runtime.Scope

/**
 * ClobType Tests
 */
class ClobTypeTest extends DataTypeFunSpec {
  implicit val ctx: QweryUniverse = QweryUniverse()
  implicit val scope: Scope = Scope()

  describe(ClobType.getClass.getSimpleName) {

    it("should encode/decode ClobType values") {
      verifyCodec(ClobType, value = "Hello World".toCharArray)
    }

    it("should resolve 'CLOB'") {
      verifySpec(spec = "CLOB", expected = ClobType)
    }

    it("should resolve 'CLOB[32]'") {
      verifySpec(spec = "CLOB[32]", expected = ArrayType(ClobType, capacity = Some(32)))
    }

    it("should provide a SQL representation") {
      verifySQL("CLOB", ClobType)
    }
  }

}
