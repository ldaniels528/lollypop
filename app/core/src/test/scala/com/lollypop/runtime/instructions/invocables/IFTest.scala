package com.lollypop.runtime.instructions.invocables

import com.lollypop.language._
import com.lollypop.language.implicits._
import com.lollypop.runtime.implicits.risky._
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.queryables.ProcedureCall
import com.lollypop.runtime.{DatabaseObjectRef, LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class IFTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[IF].getSimpleName) {

    it("should support being compiled") {
      val results = compiler.compile(
        """|if(value > 99) call doSomeThingGood(value) else call doSomeThingNotGood(value)
           |""".stripMargin)
      assert(results ==
        IF("value".f > 99,
          ProcedureCall(DatabaseObjectRef("doSomeThingGood"), args = List("value".f)),
          ProcedureCall(DatabaseObjectRef("doSomeThingNotGood"), args = List("value".f))
        ))
    }

    it("should support being decompiled") {
      verify(
        """|if(value > 99) call doSomeThingGood(value) else call doSomeThingNotGood(value)
           |""".stripMargin)
    }

    it(s"should support IF statements") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        """|var value: Int = 99
           |if(value >= 100)
           |  select rating: 'Excellent', score: value
           |else if(value >= 50)
           |  select rating: 'Satisfactory', score: value
           |else
           |  select rating: 'Unsatisfactory', score: value
           |""".stripMargin)
      device.tabulate() foreach logger.info
      assert(device.toMapGraph == List(Map("rating" -> "Satisfactory", "score" -> 99)))
    }

  }

}
