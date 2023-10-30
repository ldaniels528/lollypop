package com.lollypop.runtime.instructions.jvm

import com.lollypop.LollypopException
import com.lollypop.language.models.Expression.implicits.LifestyleExpressions
import com.lollypop.runtime.instructions.expressions.NamedFunctionCall
import com.lollypop.runtime.{LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class InvokeVirtualMethodTest extends AnyFunSpec {
  implicit val compiler: LollypopCompiler = LollypopCompiler()

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
      val (_, _, result) = LollypopVM.searchSQL(Scope(),
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
      val (_, _, result) = LollypopVM.searchSQL(Scope(),
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
      assertThrows[LollypopException] {
        LollypopVM.searchSQL(Scope(),
          """|val items = values ("NASDAQ", 1276), ("AMEX", 1259), ("NYSE", 1275), ("OTCBB", 1190)
             |items.!123
             |""".stripMargin)
      }
    }

  }

}
