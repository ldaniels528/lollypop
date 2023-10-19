package com.qwery.runtime.instructions.queryables

import com.qwery.language.QweryUniverse
import com.qwery.util.StringHelper.StringEnrichment
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

class HelpTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)
  private implicit val ctx: QweryUniverse = QweryUniverse()

  describe(classOf[Help].getSimpleName) {

    it("should successfully compile/decompile examples for all documented instructions") {
      ctx.helpDocs.zipWithIndex.foreach { case (help, n) =>
        logger.info(f"[${n + 1}%03d] ${help.name}: ${help.example.singleLine}")

        // description shouldn't be empty
        assert(help.description.trim.nonEmpty)

        // example should be compiled/decompiled to the same SQL code
        Try {
          val model0 = ctx.compiler.compile(help.example)
          val sql = model0.toSQL
          logger.info(s"SQL: $sql")
          val model1 = ctx.compiler.compile(sql)
          assert(model0 == model1)
        } match {
          case Failure(e) => fail(e.getMessage, e)
          case Success(_) =>
        }
      }
    }

  }

}
