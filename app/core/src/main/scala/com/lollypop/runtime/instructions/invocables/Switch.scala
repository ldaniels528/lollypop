package com.lollypop.runtime.instructions.invocables

import com.lollypop.language.HelpDoc.{CATEGORY_TRANSFORMATION, PARADIGM_FUNCTIONAL}
import com.lollypop.language.models._
import com.lollypop.language.{ExpressionParser, HelpDoc, InvokableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.runtime.instructions.conditions.{EQ, RuntimeCondition}
import com.lollypop.runtime.instructions.invocables.Switch.keyword
import com.lollypop.runtime.{LollypopVM, Scope}
import com.lollypop.util.OptionHelper.OptionEnrichment
import lollypop.io.IOCost

/**
 * Switch-Case Statement
 * @param term  the [[Expression expression]] to evaluate
 * @param cases the [[Switch.Case cases]]
 * @example {{{
 * switch(value)
 *    case n => n < 5 then 'Yes'
 *    case n => n == 5 then 'Maybe'
 *    case _ then 'No'
 * }}}
 * @example {{{
 * switch(stock)
 *    case p => p matches Stock(_ => true, "AMEX", _ => true) ~> .4
 *    case p => p matches Stock(_ => true, "NYSE", _ => true) ~> .6
 *    case p => p matches Stock(_ => true, "NASDAQ", _ => true) ~> .5
 *    case _ ~> 0
 * }}}
 */
case class Switch(term: Expression, cases: List[Switch.Case]) extends RuntimeInvokable
  with Expression with Queryable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    import RuntimeCondition.isTrue
    var innerScope: Scope = scope

    def isSatisfied(lf: LambdaFunction): Boolean = {
      val (sc, _, rv) = LollypopVM.execute(scope, lf.call(List(term)))
      innerScope = sc
      rv == true
    }

    (cases collectFirst {
      // lambda: case n => n < 5.0 then 'Yes'
      case Switch.Case(lf: LambdaFunction, action) if isSatisfied(lf) =>
        LollypopVM.execute(innerScope, action)
      // literal: case n then n * 2
      case Switch.Case(value: Literal, action) if isTrue(EQ(term, value)) =>
        LollypopVM.execute(scope, action)
      // condition: case lvl > 5 then strength + 1
      case Switch.Case(cond: Condition, action) if isTrue(cond) =>
        LollypopVM.execute(scope, action)
      // pass-through: case n then n * 2
      case Switch.Case(ref: IdentifierRef, action) =>
        LollypopVM.execute(scope.withVariable(ref.name, code = term, isReadOnly = true), action)
    }) || (scope, IOCost.empty, null)
  }

  override def toSQL: String = (keyword :: term.toSQL :: cases.map(_.toSQL)).mkString(" ")

}

object Switch extends ExpressionParser with InvokableParser {
  private val keyword = "switch"
  private val templateCard =
    s"""|$keyword %e:expression %OO {{
        |?case +?%e:valueExpr +?%C(t|then|~>) +?%i:thenExpr
        |}}
        |""".stripMargin

  override def help: List[HelpDoc] = {
    List(HelpDoc(
      name = keyword,
      category = CATEGORY_TRANSFORMATION,
      paradigm = PARADIGM_FUNCTIONAL,
      syntax = templateCard,
      description = "Scala-inspired switch-case statement",
      example =
        """|value = 5.7
           |switch value
           |    case n => n < 5.0 then 'Yes - {{n}}'
           |    case n => n >= 5.0 and n <= 6.0 then 'Maybe - {{n}}'
           |    case n then 'No - {{n}}'
           |""".stripMargin
    ), HelpDoc(
      name = keyword,
      category = CATEGORY_TRANSFORMATION,
      paradigm = PARADIGM_FUNCTIONAL,
      syntax = templateCard,
      description = "Scala-inspired switch-case statement",
      example =
        """|class StockQ(symbol: String, exchange: String, lastSale: Double)
           |switch new StockQ("ABC", "AMEX", 78.23)
           |    case p => p matches StockQ("ABC", "AMEX", _ => true) ~> p.lastSale
           |    case _ ~> 0.0
           |""".stripMargin
    ), HelpDoc(
      name = keyword,
      category = CATEGORY_TRANSFORMATION,
      paradigm = PARADIGM_FUNCTIONAL,
      syntax = templateCard,
      description = "Scala-inspired switch-case statement",
      example =
        """|class StockQ(symbol: String, exchange: String, lastSale: Double)
           |switch new StockQ('YORKIE', 'NYSE', 999.99)
           |    case p => p matches StockQ(_ => true, "OTCBB", _ => true) ~> 'OT'
           |    case p => p matches StockQ(_ => true, "OTHER_OTC", _ => true) ~> 'OT'
           |    case p => p matches StockQ(_ => true, "AMEX", _ => true) ~> 'AM'
           |    case p => p matches StockQ(_ => true, "NASDAQ", _ => true) ~> 'ND'
           |    case p => p matches StockQ(_ => true, "NYSE", _ => true) ~> 'NY'
           |    case _ ~> 'NA'
           |""".stripMargin
    ))
  }

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Switch] = {
    parseInvokable(ts)
  }

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Switch] = {
    if (understands(ts)) {
      val params = SQLTemplateParams(ts, templateCard)
      Some(Switch(term = params.expressions("expression"), cases = params.repeatedSets.toList.sortBy(_._1.toInt).flatMap { case (_, listOfParams) =>
        listOfParams.map { params =>
          Switch.Case(params.expressions("valueExpr"), params.instructions("thenExpr"))
        }
      }))
    } else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

  /**
   * Represents a Case condition
   * @param expression the given [[Expression condition]]
   * @param result     the given [[Expression result expression]]
   */
  case class Case(expression: Expression, result: Instruction) extends Instruction {
    override def toSQL: String = ("case" :: expression.toSQL :: "~>" :: List(result.toSQL)).mkString(" ")
  }

}