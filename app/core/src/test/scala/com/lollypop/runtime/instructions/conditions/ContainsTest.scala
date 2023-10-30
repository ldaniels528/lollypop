package com.lollypop.runtime.instructions.conditions

import com.lollypop.language.models.Expression.implicits.{LifestyleExpressions, LifestyleExpressionsAny}
import com.lollypop.runtime.instructions.expressions.{ArrayLiteral, Dictionary}
import com.lollypop.runtime.{LollypopCompiler, Scope}
import org.scalatest.funspec.AnyFunSpec

class ContainsTest extends AnyFunSpec {
  implicit val compiler: LollypopCompiler = LollypopCompiler()
  implicit val scope: Scope = Scope()

  describe(classOf[Contains].getSimpleName) {

    it("""should evaluate 'Contains("Hello SQL World".v, "SQL".v)' as true""") {
      assert(Contains("Hello SQL World".v, "SQL".v).isTrue)
    }

    it("""should evaluate 'Contains(phrase, "Goodbye")' as false""") {
      assert(Contains("phrase".f, "Goodbye".v).isFalse(scope.withVariable("phrase", Some("Hello"), isReadOnly = true)))
    }

    it("""should negate: Contains("Hello SQL World".v, "SQL".v)""") {
      assert(Contains("Hello SQL World".v, "SQL".v).negate == Not(Contains("Hello SQL World".v, "SQL".v)))
    }

    it("""should decompile 'Contains("Hello SQL World".v, "SQL".v)' to SQL""") {
      assert(Contains("Hello SQL World".v, "SQL".v).toSQL == """"Hello SQL World" contains "SQL"""")
    }

    it("""should compile: [{"name":"Tom"}] contains {"name":"Tom"}""") {
      val source = ArrayLiteral(Dictionary("name" -> "Tom".v))
      val target = Dictionary("name" -> "Tom".v)
      val model = Contains(source, target)
      assert(model.isTrue)
    }

    it("""should compile: {"name":"Tom"} contains "name" """) {
      val source = Dictionary("name" -> "Tom".v)
      val target = "name".v
      val model = Contains(source, target)
      assert(model.isTrue)
    }

    it("""should compile: "Hello World" contains "World" """) {
      implicit val scope: Scope = Scope()
      val source = "Hello World".v
      val target = "World".v
      val model = Contains(source, target)
      assert(model.isTrue)
    }

  }

}
