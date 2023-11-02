package com.lollypop.runtime.instructions.invocables

import com.lollypop.language.HelpDoc.{CATEGORY_CONTROL_FLOW, PARADIGM_FUNCTIONAL}
import com.lollypop.language.models.Expression.implicits.LifestyleExpressionsAny
import com.lollypop.language.models.{Instruction, LambdaFunction}
import com.lollypop.language.{HelpDoc, InvokableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.runtime.{LollypopVM, Scope}
import com.lollypop.util.ResourceHelper._
import lollypop.io.IOCost

/**
 * With Statement
 * @example {{{
 * with tableOpen("temp.demo.stocks") { stocks => @@stocks where lastSale < 5 }
 * }}}
 */
case class With(resource: Instruction, code: Instruction) extends RuntimeInvokable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    // execute the resource and code reference
    val (_, costR, valueR) = LollypopVM.execute(scope, resource)
    val (scopeF, costF, valueF) = LollypopVM.execute(scope, code)

    valueR match {
      case null => null
      case autoCloseable: AutoCloseable =>
        autoCloseable.use { res =>
          // execute the code reference
          valueF match {
            // was a Lambda function returned
            case lf: LambdaFunction =>
              val (scope3, cost3, value3) = LollypopVM.execute(scopeF, lf.call(List(res.v)))
              (scope3, costR ++ costF ++ cost3, value3)
            case value => (scopeF, costR ++ costF, value)
          }
        }
      case res => resource.dieResourceNotAutoCloseable(res)
    }
  }

  override def toSQL: String = List("with", resource.toSQL, code.toSQL).mkString(" ")
}

object With extends InvokableParser {
  private val templateCard = "with %i:expr %i:code"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "with",
    category = CATEGORY_CONTROL_FLOW,
    paradigm = PARADIGM_FUNCTIONAL,
    syntax = templateCard,
    description = "Provides a closure over a resource; closing it upon completion.",
    example =
      """|namespace "temp.examples"
         |drop if exists `Stocks`
         |create table `Stocks` (
         |    symbol: String(8),
         |    exchange: Enum (AMEX, NASDAQ, NYSE, OTCBB, OTHEROTC),
         |    lastSale: Double
         |) containing (
         |    |------------------------------|
         |    | symbol | exchange | lastSale |
         |    |------------------------------|
         |    | AAXX   | NYSE     |    56.12 |
         |    | UPEX   | NYSE     |   116.24 |
         |    | XYZ    | AMEX     |    31.95 |
         |    | JUNK   | AMEX     |    97.61 |
         |    | ABC    | OTCBB    |    5.887 |
         |    |------------------------------|
         |)
         |
         |with ns("Stocks") { stocks => @@stocks where lastSale < 50 }
         |""".stripMargin
  ))

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[With] = {
    if (understands(ts)) {
      val p = SQLTemplateParams(ts, templateCard)
      Some(With(resource = p.instructions("expr"), code = p.instructions("code")))
    } else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "with"

}