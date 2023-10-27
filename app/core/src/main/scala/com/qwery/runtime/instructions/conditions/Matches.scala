package com.qwery.runtime.instructions.conditions

import com.qwery.language.HelpDoc.{CATEGORY_FILTER_MATCH_OPS, PARADIGM_IMPERATIVE}
import com.qwery.language.models.Expression.implicits.LifestyleExpressionsAny
import com.qwery.language.models.{Expression, LambdaFunction}
import com.qwery.language.{ExpressionToConditionPostParser, HelpDoc, SQLCompiler, TokenStream}
import com.qwery.runtime.devices.QMap
import com.qwery.runtime.instructions.conditions.Matches.keyword
import com.qwery.runtime.instructions.functions.{AnonymousFunction, AnonymousNamedFunction}
import com.qwery.runtime.{QweryVM, Scope}
import com.qwery.util.JVMSupport.NormalizeAny

/**
 * Matches: Advanced Pattern Matching Operator
 * @param expression the [[Expression expression]] to evaluate
 * @param pattern the pattern [[Expression expression]]
 * @example {{{
 *   "Hello World" matches "H% W%"
 * }}}
 * @example {{{
 *   isNumeric = x => x.isNumber()
 *   5678 matches isNumeric
 * }}}
 * @example {{{
 *   response = { id: 5678, symbol: "DOG", exchange: "NYSE", lastSale: 90.67 }
 *   isExchange = s => s in ['NYSE', 'AMEX', 'NASDAQ', 'OTCBB']
 *   isNumber = x => x.isNumber()
 *   isString = x => x.isString()
 *   response matches { id: isNumber, symbol: isString, exchange: isExchange, lastSale: isNumber }
 * }}}
 */
case class Matches(expression: Expression, pattern: Expression) extends RuntimeCondition {
  override def isTrue(implicit scope: Scope): Boolean = {
    val (_, _, aValue) = QweryVM.execute(scope, expression)
    val (_, _, aPattern) = QweryVM.execute(scope, pattern)
    aPattern match {
      // "Hello World" matches "H.* W.*"
      case textPattern: String =>
        aValue match {
          case text: String => text.matches(textPattern)
          case z => pattern.dieIllegalType(z)
        }
      // 5678 matches isNumeric
      case function: LambdaFunction => isMatch(src = aValue, pattern = function)
      // response matches { id: isNumber, symbol: isString, exchange: isExchange, lastSale: isNumber }
      case objectPattern => isMatch(src = aValue, pattern = objectPattern)
    }
  }

  override def toSQL: String = s"${expression.toSQL} ${keyword} ${pattern.toSQL}"

  private def isMatch(src: Any, pattern: Any)(implicit scope: Scope): Boolean = {
    (src.normalizeArrays, pattern.normalizeArrays) match {
      case (a: QMap[String, _], b: QMap[String, _]) => isMatchMap(a, b)
      case (a: QMap[String, _], b: Seq[_]) => isMatchSeq(a.toSeq, b)
      case (a: Seq[_], b: QMap[String, _]) => isMatchSeq(a, b.toSeq)
      case (a: Seq[_], b: Seq[_]) => isMatchSeq(a, b)
      case (a, b: AnonymousFunction) => QweryVM.execute(scope, b.call(List(a.v)))._3 == true
      case (a, b) => a == b
    }
  }

  private def isMatchMap(src: QMap[String, Any], pattern: QMap[String, Any])(implicit scope: Scope): Boolean = {
    if (src.keys != pattern.keys) false
    else {
      val results = pattern map {
        case (name: String, af: AnonymousFunction) =>
          src.get(name).exists { value => QweryVM.execute(scope, af.call(List(value.v)))._3 == true }
        case (name: String, af: AnonymousNamedFunction) =>
          src.get(name).exists { value => af.call(List(value.v)).evaluate() == true }
        case (name: String, value) => src.get(name).exists(isMatch(_, value))
      }
      results.forall(_ == true)
    }
  }

  private def isMatchSeq(src: Seq[_], pattern: Seq[_])(implicit scope: Scope): Boolean = {
    if (src.size != pattern.size) false
    else {
      val results = src zip pattern map { case (a, b) => isMatch(a, b) }
      results.forall(_ == true)
    }
  }

}

object Matches extends ExpressionToConditionPostParser {
  private val keyword = "matches"
  private val templateCard = s"%e:source $keyword %e:target"

  override def parseConditionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[Matches] = {
    if (ts.nextIf(keyword)) compiler.nextExpression(ts).map(Matches(host, _)) else None
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = keyword,
    category = CATEGORY_FILTER_MATCH_OPS,
    paradigm = PARADIGM_IMPERATIVE,
    syntax = templateCard,
    description = "determines whether the `value` matches the `expression`",
    example =
      """|"Hello World" matches "H.* W.*"
         |""".stripMargin
  ), HelpDoc(
    name = keyword,
    category = CATEGORY_FILTER_MATCH_OPS,
    paradigm = PARADIGM_IMPERATIVE,
    syntax = templateCard,
    description = "determines whether the `value` matches the `expression`",
    example =
      """|isNumeric = x => x.isNumber()
         |5678 matches isNumeric
         |""".stripMargin
  ), HelpDoc(
    name = keyword,
    category = CATEGORY_FILTER_MATCH_OPS,
    paradigm = PARADIGM_IMPERATIVE,
    syntax = templateCard,
    description = "determines whether the `value` matches the `expression`",
    example =
      """|response = { id: 5678, symbol: "DOG", exchange: "NYSE", lastSale: 90.67 }
         |isExchange = s => s in ['NYSE', 'AMEX', 'NASDAQ', 'OTCBB']
         |isNumber = x => x.isNumber()
         |isString = x => x.isString()
         |response matches { id: isNumber, symbol: isString, exchange: isExchange, lastSale: isNumber }
         |""".stripMargin
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}