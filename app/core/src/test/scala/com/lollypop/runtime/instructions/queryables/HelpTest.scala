package com.lollypop.runtime.instructions.queryables

import com.lollypop.language.models.Instruction
import com.lollypop.language.{LollypopUniverse, _}
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

class HelpTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)
  private implicit val ctx: LollypopUniverse = LollypopUniverse()

  describe(classOf[Help].getSimpleName) {

    it("should successfully compile/decompile examples for all documented instructions") {
      ctx.helpDocs.zipWithIndex.foreach { case (help, n) =>
        logger.info(f"[${n + 1}%03d] ${help.name}: ${help.example.singleLine}")

        // description shouldn't be empty
        assert(help.description.trim.nonEmpty)

        // example should be compiled/decompiled to the same SQL code
        var model0: Instruction = null
        var model1: Instruction = null
        Try {
          model0 = ctx.compiler.compile(help.example)
          val sql = model0.toSQL
          logger.info(s"SQL: $sql")
          model1 = ctx.compiler.compile(sql)
          assert(model0 == model1)
        } match {
          case Failure(e) =>
            logger.info(s"model0: ${model0}")
            logger.info(s"model1: ${model1}")
            fail(e.getMessage, e)
          case Success(_) =>
        }
      }
    }

  }

}
