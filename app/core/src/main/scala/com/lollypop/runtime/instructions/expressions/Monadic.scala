package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.HelpDoc.{CATEGORY_TRANSFORMATION, PARADIGM_FUNCTIONAL}
import com.lollypop.language.models.Expression.implicits.LifestyleExpressionsAny
import com.lollypop.language.models.{Expression, LambdaFunction}
import com.lollypop.language.{ExpressionChainParser, HelpDoc, SQLCompiler, TokenStream}
import com.lollypop.runtime.LollypopVM.implicits.InstructionExtensions
import com.lollypop.runtime.Scope
import com.lollypop.runtime.instructions.expressions.Monadic.__symbol
import com.lollypop.util.OptionHelper.OptionEnrichment
import lollypop.io.IOCost

import scala.concurrent.Future
import scala.util.Try

/**
 * Monadic Operator (=>>)
 * @param expr0 the left-side expression
 * @param expr1 the right-side expression
 * @example {{{
 *  import "scala.util.Success"
 *  a = Success(75)
 *  b = Success(25)
 *  c = a =>> i => i * 2
 *  c
 * }}}
 */
case class Monadic(expr0: Expression, expr1: Expression) extends RuntimeExpression {
  import scala.concurrent.ExecutionContext.Implicits.global

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    val monad = expr0.execute(scope)._3
    val result = expr1.execute(scope)._3 match {
      case lf: LambdaFunction =>
        val exec: Expression => Any = v => lf.call(List(v)).execute(scope)._3
        monad match {
          case f: Future[_] => f.map(v => exec(v.v))
          case o: Option[_] => o.map(v => exec(v.v))
          case t: Try[_] => t.map(v => exec(v.v))
          case z => expr1.dieIllegalType(z)
        }
      case z => expr1.dieIllegalType(z)
    }
    (scope, IOCost.empty, result)
  }

  override def toSQL: String = Seq(expr0.toSQL, __symbol, expr1.toSQL).mkString(" ")

}

object Monadic extends ExpressionChainParser {
  private val __symbol = "=>>"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = __symbol,
    category = CATEGORY_TRANSFORMATION,
    paradigm = PARADIGM_FUNCTIONAL,
    syntax = "",
    description = "Monadic comprehension",
    example =
      """|import "scala.util.Success"
         |a = Success(75)
         |b = Success(25)
         |c = a =>> i => i * 2
         |c
         |""".stripMargin,
    isExperimental = true
  ), HelpDoc(
    name = __symbol,
    category = CATEGORY_TRANSFORMATION,
    paradigm = PARADIGM_FUNCTIONAL,
    syntax = "",
    description = "Monadic comprehension",
    example =
      """|import "scala.util.Success"
         |a = Success(75)
         |b = Success(25)
         |c = a =>> i =>
         |    b =>> j => i + j
         |c
         |""".stripMargin,
    isExperimental = true
  ))

  override def parseExpressionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[Expression] = {
    if (ts.nextIf(__symbol)) {
      compiler.nextExpression(ts).map(Monadic(host, _)) ?? ts.dieExpectedExpression()
    } else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is __symbol

}