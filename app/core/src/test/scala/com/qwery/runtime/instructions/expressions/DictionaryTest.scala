package com.qwery.runtime.instructions.expressions

import com.qwery.language.models.Expression.implicits.{LifestyleExpressionsAny, map2Expr}
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.{QweryCompiler, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

import scala.collection.mutable

class DictionaryTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[Dictionary].getSimpleName) {

    it("should render models as SQL") {
      val model = Dictionary("message1" -> "Hello World".v, "message2" -> "Hallo Monde".v)
      assert(model.toSQL == """{ message1: "Hello World", message2: "Hallo Monde" }""")
    }

    it("should create and mutate dictionaries") {
     val result = QweryVM.executeSQL(Scope(),
        """|response = { 'message1' : 'Hello World' }
           |response.message2 = 'Hallo Monde'
           |response
           |""".stripMargin)._3
      assert(result == mutable.LinkedHashMap("message1" -> "Hello World", "message2" -> "Hallo Monde"))
    }

  }

}
