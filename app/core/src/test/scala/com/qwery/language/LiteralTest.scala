package com.qwery.language

import com.qwery.language.models.Expression.implicits.LifestyleExpressionsAny
import com.qwery.language.models.Literal
import com.qwery.runtime.QweryCompiler
import com.qwery.runtime.instructions.expressions.{ArrayLiteral, Dictionary}
import org.scalatest.funspec.AnyFunSpec

class LiteralTest extends AnyFunSpec {
  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[Literal].getSimpleName) {

    it("should evaluate: 0b1111111111111111111111111111111") {
      assert(compiler.compile("0b1111111111111111111111111111111") == 2147483647.v)
    }

    it("should evaluate: 0b11111111111111111111111111111111") {
      assert(compiler.compile("0b11111111111111111111111111111111") == 4294967295L.v)
    }

    it("should evaluate: 0x7fffffff") {
      assert(compiler.compile("0x7fffffff") == 2147483647.v)
    }

    it("should evaluate: 0xdeadbeef") {
      assert(compiler.compile("0xdeadbeef") == 3735928559L.v)
    }

    it("should evaluate: 0o17777777777") {
      assert(compiler.compile("0o17777777777") == 2147483647.v)
    }

    it("should evaluate: 0o27777777777") {
      assert(compiler.compile("0o27777777777") == 3221225471L.v)
    }

    it("should evaluate: 3d") {
      assert(compiler.compile("3d") == 3d.v)
    }

    it("should evaluate: 3f") {
      assert(compiler.compile("3f") == 3f.v)
    }

    it("should evaluate: 3L") {
      assert(compiler.compile("3L") == 3L.v)
    }

    it("should evaluate: 3s") {
      assert(compiler.compile("3s") == 3.toShort.v)
    }

    it("should evaluate: false") {
      assert(compiler.compile("false") == false.v)
    }

    it("should evaluate: null") {
      assert(compiler.compile("null") == qwery.lang.Null())
    }

    it("should evaluate: true") {
      assert(compiler.compile("true") == true.v)
    }

    it("should evaluate: [1l, 2l, 3l]") {
      assert(compiler.compile("[1l, 2l, 3l]") == ArrayLiteral(1L.v, 2L.v, 3L.v))
    }

    it("should evaluate: {a: 1, b: 2, c: 3}") {
      assert(compiler.compile("{a: 1, b: 2, c: 3}") == Dictionary("a" -> 1.v, "b" -> 2.v, "c" -> 3.v))
    }

  }

}
