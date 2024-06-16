package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.LifestyleExpressionsAny
import com.lollypop.runtime.{LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class SpanTest extends AnyFunSpec {
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[Span.Exclusive].getSimpleName) {

    it("should compile an exclusive range") {
      val model = compiler.compile("1 until 5")
      assert(model == Span.Exclusive(1.v, 5.v))
    }

    it("should decompile an exclusive range") {
      val model = Span.Exclusive(1.v, 5.v)
      assert(model.toSQL == "1 until 5")
    }

    it("should execute an exclusive range") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(), "1 until 5")
      val list = result match {
        case a: Array[_] => a.toList
        case _ => Nil
      }
      assert(list == List(1, 2, 3, 4))
    }

  }

  describe(classOf[Span.Inclusive].getSimpleName) {

    it("should compile an inclusive range") {
      val model = compiler.compile("1 to 5")
      assert(model == Span.Inclusive(1.v, 5.v))
    }

    it("should decompile an inclusive range") {
      val model = Span.Inclusive(1.v, 5.v)
      assert(model.toSQL == "1 to 5")
    }

    it("should execute an inclusive range") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(), "1 to 5")
      val list = result match {
        case a: Array[_] => a.toList
        case _ => Nil
      }
      assert(list == List(1, 2, 3, 4, 5))
    }

  }

}
