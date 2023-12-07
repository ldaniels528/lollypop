package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.LifestyleExpressionsAny
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.{LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

import scala.collection.mutable

class DictionaryTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[Dictionary].getSimpleName) {

    it("should render models as SQL") {
      val model = Dictionary("message1" -> "Hello World".v, "message2" -> "Hallo Monde".v)
      assert(model.toSQL == """{ message1: "Hello World", message2: "Hallo Monde" }""")
    }

    it("should create and mutate dictionaries") {
     val result = LollypopVM.executeSQL(Scope(),
        """|response = { 'message1' : 'Hello World' }
           |response.message2 = 'Hallo Monde'
           |response
           |""".stripMargin)._3
      assert(result == mutable.LinkedHashMap("message1" -> "Hello World", "message2" -> "Hallo Monde"))
    }

  }

}
