package com.qwery.runtime.instructions.jvm

import com.qwery.QweryException
import com.qwery.language.models.Expression.implicits.LifestyleExpressions
import com.qwery.runtime.instructions.expressions.NamedFunctionCall
import com.qwery.runtime.{QweryCompiler, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class InvokeVirtualMethodTest extends AnyFunSpec {
  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[InvokeVirtualMethod].getSimpleName) {

    it("should compile: value.!toTable()") {
      val model = compiler.compile("value.!toTable()")
      assert(model == InvokeVirtualMethod("value".f, NamedFunctionCall("toTable")))
    }

    it("should decompile: value.!toTable()") {
      val model = InvokeVirtualMethod("value".f, NamedFunctionCall("toTable"))
      assert(model.toSQL == "value.!toTable()")
    }

    it("should execute: items.!toTable()") {
      val (_, _, result) = QweryVM.searchSQL(Scope(),
        """|val items = values ("NASDAQ", 1276), ("AMEX", 1259), ("NYSE", 1275), ("OTCBB", 1190)
           |items.!toTable()
           |""".stripMargin)
      assert(result.toMapGraph == List(
        Map("A" -> "NASDAQ", "B" -> 1276),
        Map("A" -> "AMEX", "B" -> 1259),
        Map("A" -> "NYSE", "B" -> 1275),
        Map("A" -> "OTCBB", "B" -> 1190)
      ))
    }

    it("should execute: items.toTable(__scope__)") {
      val (_, _, result) = QweryVM.searchSQL(Scope(),
        """|val items = values ("NASDAQ", 1276), ("AMEX", 1259), ("NYSE", 1275), ("OTCBB", 1190)
           |items.toTable(__scope__)
           |""".stripMargin)
      assert(result.toMapGraph == List(
        Map("A" -> "NASDAQ", "B" -> 1276),
        Map("A" -> "AMEX", "B" -> 1259),
        Map("A" -> "NYSE", "B" -> 1275),
        Map("A" -> "OTCBB", "B" -> 1190)
      ))
    }

    it("should reject: items.!123") {
      assertThrows[QweryException] {
        QweryVM.searchSQL(Scope(),
          """|val items = values ("NASDAQ", 1276), ("AMEX", 1259), ("NYSE", 1275), ("OTCBB", 1190)
             |items.!123
             |""".stripMargin)
      }
    }

  }

}
