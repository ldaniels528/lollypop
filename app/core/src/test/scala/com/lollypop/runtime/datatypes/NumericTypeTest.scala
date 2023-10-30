package com.lollypop.runtime.datatypes

import com.lollypop.language.LollypopUniverse
import com.lollypop.runtime.Scope
import com.lollypop.runtime.datatypes.DataTypeFunSpec.implicits.DataTypeConversion

import java.nio.ByteBuffer

class NumericTypeTest extends DataTypeFunSpec {
  implicit val ctx: LollypopUniverse = LollypopUniverse()
  implicit val scope: Scope = Scope()

  describe(classOf[NumericType].getSimpleName) {

    it("should detect BigDecimal column types") {
      verifyType(value = BigDecimal(123.456), expectedType = NumericType)
      verifyType(value = new java.math.BigDecimal("123.456"), expectedType = NumericType)
    }

    it("should encode/decode BigDecimal values") {
      verifyCodec(NumericType, value = 12345.6789d)
    }

    it("should encode/decode BigInt values") {
      verifyCodec(NumericType, value = BigInt(123456).bigInteger)
    }

    it("should conversions to BigInt") {
      assert("1000".convertTo(NumericType) == 1000)
    }

    it("should be encode/decode a BigDecimal") {
      val expect = new java.math.BigDecimal(Double.MaxValue)
      val bytes = NumericType.encode(expect)
      info(s"encoded size: ${bytes.length}")
      val actual = NumericType.decode(ByteBuffer.wrap(bytes))
      assert(actual == expect)
    }

    it("should be encode/decode a BigInteger") {
      val expect = new java.math.BigInteger(String.valueOf(Long.MaxValue))
      val bytes = NumericType.encode(expect)
      info(s"encoded size: ${bytes.length}")
      val actual = NumericType.decode(ByteBuffer.wrap(bytes))
      assert(actual == expect)
    }

  }

}
