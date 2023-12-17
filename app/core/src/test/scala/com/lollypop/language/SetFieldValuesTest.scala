package com.lollypop.language

import com.lollypop.runtime.LollypopCompiler
import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec

class SetFieldValuesTest extends AnyFunSpec {
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[SetFieldValues].getSimpleName) {

    it("should parse key-value-pairs tags (%U)") {
      verify(text = "comments = 'Raise the price'", template = "%U:assignments")(SQLTemplateParams(assignments = Map(
        "assignments" -> List("comments" -> "Raise the price".v)
      )))
    }

  }

  def verify(text: String, template: String)(expected: SQLTemplateParams): Assertion = {
    info(s"'$template' <~ '$text'")
    val actual = SQLTemplateParams(TokenStream(text), template)
    println(s"actual:   ${actual.parameters}")
    println(s"expected: ${expected.parameters}")
    assert(actual == expected, s"'$text' ~> '$template' failed")
  }

}
