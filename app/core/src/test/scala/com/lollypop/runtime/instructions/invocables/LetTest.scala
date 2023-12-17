package com.lollypop.runtime.instructions.invocables

import com.lollypop.language._
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.functions.AnonymousNamedFunction
import com.lollypop.runtime.{LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class LetTest extends AnyFunSpec with VerificationTools {

  describe(classOf[Let].getSimpleName) {

    it("should decompile Let instructions") {
      val model = Let("r", codec = AnonymousNamedFunction("roman"), initialValue = 98.v)
      assert(model.toSQL == "let r : roman = 98")
    }

    it("should evaluate: let b64 : base64 = 'Hello'") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|val base64 = (value: String) => String(value.getBytes().base64())
           |let b64 : base64 = "Hello"
           |b64
           |""".stripMargin)
      assert(result == "SGVsbG8=")
    }

    it("should evaluate: this where name is 'b64'") {
      val (_, _, result) = LollypopVM.searchSQL(Scope(),
        """|val base64 = (value: String) => String(value.getBytes().base64())
           |let b64 : base64 = "Hello"
           |from this where name is "b64"
           |""".stripMargin)
      assert(result.toMapGraph == List(Map("name" -> "b64", "kind" -> "String", "value" -> "\"SGVsbG8=\"")))
    }

    it("should evaluate: let b : binary = 78") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|val binary = (value: Long) => value.asBinaryString()
           |let b : binary = 78
           |b
           |""".stripMargin)
      assert(result == "1001110")
    }

    it("should evaluate: let f : factorial = 4") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|def factorial(n: Int) := iff(n <= 1, 1, n * factorial(n - 1))
           |let f : factorial = 4
           |f
           |""".stripMargin)
      assert(result == 24.0)
    }

    it("should evaluate: let m : md5 = ...") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|val md5 = (value: String) => value.getBytes().md5().toHex()
           |let m : md5 = "the brown fox did what?"
           |m
           |""".stripMargin)
      assert(result == "63f685c9a4868d0bd2028c61b597547f")
    }

    it("should evaluate: let r : roman = 98") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|def roman(value: Int) := ("I" * value)
           |  .replaceAll("IIIII", "V")
           |  .replaceAll("IIII", "IV")
           |  .replaceAll("VV", "X")
           |  .replaceAll("VIV", "IX")
           |  .replaceAll("XXXXX", "L")
           |  .replaceAll("XXXX", "XL")
           |  .replaceAll("LL", "C")
           |  .replaceAll("LXL", "XC")
           |  .replaceAll("CCCCC", "D")
           |  .replaceAll("CCCC", "CD")
           |  .replaceAll("DD", "M")
           |  .replaceAll("DCD", "CM")
           |
           |let r : roman = 98
           |r
           |""".stripMargin)
      assert(result == "XCVIII")
    }

  }

}
