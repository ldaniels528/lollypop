package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.models.Expression.implicits._
import com.lollypop.language.models.Inequality.InequalityExtensions
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.invocables.IF
import com.lollypop.runtime.{LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class IfTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[IF].getSimpleName) {

    it("should decompile: if(value >= 100) 'Y'") {
      assert(IF("value".f >= 100.v, 'Y'.v, None).toSQL == """if(value >= 100) 'Y'""")
    }

    it("should decompile: if(value >= 100) 'Y' else 'N'") {
      assert(IF("value".f >= 100.v, 'Y'.v, Some('N'.v)).toSQL == """if(value >= 100) 'Y' else 'N'""")
    }

    it("should execute: if(value > 99) 'Y' else 'N'") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|val value = 100
           |if(value > 99) 'Y' else 'N'
           |""".stripMargin)
      assert(result == 'Y')
    }

    it(s"should execute complex expressions") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        """|val value: Int = 99
           |val rating =
           |  if(value >= 100) 'Excellent'
           |  else if(value >= 50) 'Satisfactory'
           |  else 'Unsatisfactory'
           |select rating, score: value
           |""".stripMargin)
      device.tabulate() foreach logger.info
      assert(device.toMapGraph == List(Map("rating" -> "Satisfactory", "score" -> 99)))
    }

  }

}
