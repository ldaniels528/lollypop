package com.qwery.runtime.instructions.functions

import com.qwery.language.models.Expression.implicits.{LifestyleExpressions, LifestyleExpressionsAny}
import com.qwery.language.models.Inequality.InequalityExtensions
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.{QweryCompiler, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class IffTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)
  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[Iff].getSimpleName) {

    it("""should parse "iff(lastSale <= 1.0, "Y", "N")"""") {
      verify("""iff(lastSale <= 1.0, "Y", "N")""", Iff("lastSale".f <= 1.0.v, "Y".v, "N".v))
    }

    it("should parse: iff(LastSale < 1, 'Penny Stock', 'Stock')") {
      verify("iff(LastSale < 1, 'Penny Stock', 'Stock')", Iff("LastSale".f < 1.0.v, "Penny Stock".v, "Stock".v))
    }

    it("should support being executed") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|val value = 100
           |iff(value > 99, 'Y', 'N')
           |""".stripMargin)
      assert(result == 'Y')
    }

    it(s"should execute IF expressions") {
      val (_, _, device) = QweryVM.searchSQL(Scope(),
        """|val value: Int = 99
           |select iff(value >= 100, 'Excellent', iff(value >= 50, 'Satisfactory', 'Unsatisfactory')) as rating, value as score
           |""".stripMargin)
      device.tabulate() foreach logger.info
      assert(device.toMapGraph == List(Map("rating" -> "Satisfactory", "score" -> 99)))
    }

  }

}
