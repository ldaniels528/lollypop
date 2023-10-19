package com.qwery.runtime.instructions.conditions

import com.qwery.language.models.Expression.implicits._
import com.qwery.runtime.Scope
import org.scalatest.funspec.AnyFunSpec

class LikeTest extends AnyFunSpec {
  implicit val scope: Scope = Scope()

  describe(classOf[Like].getSimpleName) {

    it("""should evaluate true: like("hello", "h%llo")""") {
      assert(Like("hello", "h%llo").isTrue)
    }

    it("""should evaluate false: like("hello!", "h%llo")""") {
      assert(Like("hello!", "h%llo").isFalse)
    }

    it("""should negate: like(a, "hello")""") {
      assert(Like("a".f, "hello").negate == Not(Like("a".f, "hello")))
    }

    it("""should negate: not(like(a, "hello"))""") {
      assert(Not(Like("a".f, "hello")).negate == Like("a".f, "hello"))
    }

    it("""should decompile: like(name, "Lawr%")""") {
      assert(Like("name".f, "Lawr%").toSQL == """name like "Lawr%"""")
    }

  }

}
