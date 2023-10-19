package com.qwery.runtime.instructions.conditions

import com.qwery.language.models.Expression.implicits._
import com.qwery.runtime.Scope
import org.scalatest.funspec.AnyFunSpec

class RLikeTest extends AnyFunSpec {
  implicit val scope: Scope = Scope()

  describe(classOf[RLike].getSimpleName) {

    it("""should evaluate true: RLike("hello", "h.*llo")""") {
      assert(RLike("hello", "h.*llo").isTrue)
    }

    it("""should evaluate false: RLike("hello!", "h.*llo")""") {
      assert(RLike("hello!", "h.*llo").isFalse)
    }

    it("""should negate: RLike("a", "hello")""") {
      assert(RLike("a".f, "hello").negate == Not(RLike("a".f, "hello")))
    }

    it("""should negate: not(RLike("a", "hello"))""") {
      assert(Not(RLike("a".f, "hello")).negate == RLike("a".f, "hello"))
    }

    it("""should decompile: RLike(name, "Lawr(.*))""") {
      assert(RLike("name".f, "Lawr(.*)").toSQL == """name rlike "Lawr(.*)"""")
    }

  }

}
