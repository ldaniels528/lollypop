package com.qwery.runtime.datatypes

import com.qwery.language.QweryUniverse
import com.qwery.runtime.datatypes.DataTypeFunSpec.implicits.DataTypeConversion
import com.qwery.runtime.{QweryVM, Scope}

/**
 * StringType Tests
 */
class StringTypeTest extends DataTypeFunSpec {
  implicit val ctx: QweryUniverse = QweryUniverse()
  implicit val scope: Scope = Scope()

  describe(StringType.getClass.getSimpleName) {

    it("should detect StringType column types") {
      verifyType(value = "Hello World", expectedType = StringType)
    }

    it("should conversions to StringType") {
      assert(12345.convertTo(StringType) == "12345")
    }

    it("should encode/decode StringType values") {
      verifyCodec(StringType, value = "Hello World")
    }

    it("should support string interpolation") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|"This is a {{ String(['t', 'e', 's', 't']) }}"
           |""".stripMargin)
      assert(result == "This is a test")
    }

    it("should encode/decode StringType with metadata") {
      val value = "Hello World"
      val encoded = StringType.encodeFull(value)
      logger.info(s"encoded: ${encoded.array().map(b => Integer.toHexString(b.toInt)).mkString(".")}")
      val (fmd, decoded_?) = StringType.decodeFull(encoded)
      logger.info(s"decoded: ${decoded_?} [$fmd]")
      assert(decoded_? contains value)
    }

    it("should resolve 'String(32)'") {
      verifySpec(spec = "String(32)", expected = StringType(32))
    }

    it("should resolve 'String(32)[20]'") {
      verifySpec(spec = "String(32)[20]", expected = ArrayType(StringType(32), capacity = Some(20)))
    }

    it("should provide a SQL representation") {
      verifySQL("String(32)", StringType(32))
    }

  }

}
