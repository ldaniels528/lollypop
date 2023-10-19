package com.qwery.runtime.instructions.jvm

import com.qwery.language.TokenStream
import com.qwery.language.models.Expression.implicits.{LifestyleExpressions, LifestyleExpressionsAny}
import com.qwery.runtime.{QweryCompiler, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class IsCodecOfTest extends AnyFunSpec {
  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[IsCodecOf].getSimpleName) {

    it("""should compile: 35 isCodecOf Long""") {
      val result = compiler.nextExpression(TokenStream("35 isCodecOf Long"))
      assert(result contains IsCodecOf(35L.v, "Long".ct))
    }

    it("""should decompile: 35 isCodecOf Long""") {
      assert(IsCodecOf(35L.v, "Long".ct).toSQL == "35 isCodecOf Long")
    }

    it("""should evaluate: 35 isCodecOf Int""") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|35 isCodecOf Int
           |""".stripMargin
      )
      assert(result == true)
    }

    it("""should evaluate: 35 isCodecOf Double""") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|35 isCodecOf Double
           |""".stripMargin
      )
      assert(result == false)
    }

    it("""should evaluate: 35.0 isCodecOf Double""") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|35.0 isCodecOf Double
           |""".stripMargin
      )
      assert(result == true)
    }

    it("""should evaluate: "35" isCodecOf String""") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|"35" isCodecOf String(2)
           |""".stripMargin
      )
      assert(result == true)
    }

    it("""should evaluate: (new `java.util.Date`()) isCodecOf DateTime""") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|(new `java.util.Date`()) isCodecOf DateTime
           |""".stripMargin
      )
      assert(result == true)
    }

  }

}
