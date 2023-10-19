package com.qwery.runtime.datatypes

import com.qwery.language.QweryUniverse
import com.qwery.language.models.ColumnType
import com.qwery.runtime.datatypes.DataTypeFunSpec.implicits.DataTypeConversion
import com.qwery.runtime.{Scope, datatypes}

/**
 * CharType Tests
 */
class CharTypeTest extends DataTypeFunSpec {
  implicit val ctx: QweryUniverse = QweryUniverse()
  implicit val scope: Scope = Scope()

  describe(CharType.getClass.getSimpleName) {

    it("should resolve 'Char(1)' as 'CharType'") {
      assert(datatypes.DataType(ColumnType(name = "Char", size = 1)) == CharType)
    }

    it("should resolve 'Char(30)' as 'StringType'") {
      assert(datatypes.DataType(ColumnType(name = "Char", size = 30)) == StringType(30))
    }

    it("should detect CharType column types") {
      verifyType(value = ' ', expectedType = CharType)
    }

    it("should encode/decode CharType values") {
      verifyCodec(CharType, value = 'A')
    }

    it("should conversions to CharType") {
      assert('1'.convertTo(CharType) == '1')
    }

    it("should resolve 'Char'") {
      verifySpec(spec = "Char", expected = CharType)
    }

    it("should resolve 'Char[5]'") {
      verifySpec(spec = "Char[5]", expected = ArrayType(CharType, capacity = Some(5)))
    }

    it("should provide a SQL representation") {
      verifySQL("Char", CharType)
    }
  }

}
