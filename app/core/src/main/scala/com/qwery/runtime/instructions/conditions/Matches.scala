package com.qwery.runtime.instructions.conditions

import com.qwery.language.HelpDoc.{CATEGORY_FILTER_MATCH_OPS, PARADIGM_DECLARATIVE}
import com.qwery.language.models.Expression.implicits.LifestyleExpressionsAny
import com.qwery.language.models.{Expression, LambdaFunction, Literal}
import com.qwery.language.{ExpressionToConditionPostParser, HelpDoc, SQLCompiler, TokenStream}
import com.qwery.runtime.devices.QMap
import com.qwery.runtime.instructions.conditions.Matches.keyword
import com.qwery.runtime.instructions.expressions.{NamedFunctionCall, New}
import com.qwery.runtime.instructions.functions.AnonymousNamedFunction
import com.qwery.runtime.{QweryVM, Scope}
import com.qwery.util.JVMSupport.NormalizeAny

/**
 * Matches: Advanced Pattern Matching Operator
 * @param expression the [[Expression expression]] to evaluate
 * @param pattern    the pattern [[Expression expression]]
 * @example {{{
 *   "Hello World" matches "H% W%"
 * }}}
 * @example {{{
 *   isNumeric = x => x.isNumber()
 *   5678 matches isNumeric
 * }}}
 * @example {{{
 *   isExchange = s => s in ['NYSE', 'AMEX', 'NASDAQ', 'OTCBB']
 *   isNumber = x => x.isNumber()
 *   isString = x => x.isString()
 *   response = { id: 5678, symbol: "DOG", exchange: "NYSE", lastSale: 90.67 }
 *   response matches { id: isNumber, symbol: isString, exchange: isExchange, lastSale: isNumber }
 * }}}
 * @example {{{
 *   class Stock(symbol: String, exchange: String, lastSale: Double)
 *   stock = new Stock(symbol: "AAPL", exchange: "NASDAQ", lastSale: 234.57)
 *   stock matches Stock(symbol: "AAPL", exchange: "NASDAQ", lastSale: 234.57)
 * }}}
 */
case class Matches(expression: Expression, pattern: Expression) extends RuntimeCondition {
  override def isTrue(implicit scope: Scope): Boolean = {
    val (scopeA, _, aValue) = QweryVM.execute(scope, expression)
    pattern match {
      // "Hello World" matches "H.* W.*"
      case Literal(textPattern: String) =>
        aValue match {
          case text: String => text.matches(textPattern)
          case z => pattern.dieIllegalType(z)
        }
      // 5678 matches isNumeric
      case fx: LambdaFunction => isMatch(src = aValue, pattern = fx)
      // stock matches Stock(symbol: "AMD", exchange: "NASDAQ", lastSale: 67.57)
      case fx: NamedFunctionCall => isMatchProduct(instA = aValue, pattern = fx)
      // response matches { id: isNumber, symbol: isString, exchange: isExchange, lastSale: isNumber }
      case _ =>
        val (_, _, objectPattern) = QweryVM.execute(scopeA, pattern)
        isMatch(src = aValue, pattern = objectPattern)
    }
  }

  override def toSQL: String = Seq(expression.toSQL, keyword, pattern.toSQL).mkString(" ")

  private def isMatch(src: Any, pattern: Any)(implicit scope: Scope): Boolean = {
    (src.normalizeArrays, pattern.normalizeArrays) match {
      case (a: QMap[String, _], b: QMap[String, _]) => isMatchMap(a, b)
      case (a: QMap[String, _], b: Seq[_]) => isMatchSeq(a.toSeq, b)
      case (a: Seq[_], b: QMap[String, _]) => isMatchSeq(a, b.toSeq)
      case (a: Seq[_], b: Seq[_]) => isMatchSeq(a, b)
      case (a, b: LambdaFunction) => QweryVM.execute(scope, b.call(List(a.v)))._3 == true
      case (a, b) => a == b
    }
  }

  private def isMatchMap(src: QMap[String, Any], pattern: QMap[String, Any])(implicit scope: Scope): Boolean = {
    if (src.keys != pattern.keys) false
    else {
      val results = pattern map {
        case (name: String, af: LambdaFunction) =>
          src.get(name).exists { value => QweryVM.execute(scope, af.call(List(value.v)))._3 == true }
        case (name: String, af: AnonymousNamedFunction) =>
          src.get(name).exists { value => af.call(List(value.v)).evaluate() == true }
        case (name: String, value) => src.get(name).exists(isMatch(_, value))
      }
      results.forall(_ == true)
    }
  }

  private def isMatchProduct(instA: Any, pattern: NamedFunctionCall)(implicit scope: Scope): Boolean = {
    val (_, _, instB) = QweryVM.execute(scope, New(typeName = pattern.name, pattern.args))
    (instA, instB) match {
      case (a: Product, b: Product) =>
        a.productElementNames.sameElements(b.productElementNames) &&
          a.productIterator.zip(b.productIterator).forall(t => isMatch(t._1, t._2))
      case (a, b) => isMatch(a, b)
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
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "determines whether the `value` matches the `expression`",
    example =
      """|"Hello 123" matches "H.* \d+"
         |""".stripMargin
  ), HelpDoc(
    name = keyword,
    category = CATEGORY_FILTER_MATCH_OPS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "determines whether the `value` matches the `expression`",
    example =
      """|isNumeric = x => x.isNumber()
         |5678 matches isNumeric
         |""".stripMargin
  ), HelpDoc(
    name = keyword,
    category = CATEGORY_FILTER_MATCH_OPS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "determines whether the `value` matches the `expression`",
    example =
      """|isExchange = x => x in ['NYSE', 'AMEX', 'NASDAQ', 'OTCBB']
         |isNumber = x => x.isNumber()
         |isString = x => x.isString()
         |
         |response = { id: 5678, symbol: "DOG", exchange: "NYSE", lastSale: 90.67 }
         |response matches { id: isNumber, symbol: isString, exchange: isExchange, lastSale: isNumber }
         |""".stripMargin
  ), HelpDoc(
    name = keyword,
    category = CATEGORY_FILTER_MATCH_OPS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "determines whether the `value` matches the `expression`",
    example =
      """|class Stock(symbol: String, exchange: String, lastSale: Double)
         |stock = new Stock(symbol: "AAPL", exchange: "NASDAQ", lastSale: 234.57)
         |stock matches Stock(symbol: "AAPL", exchange: "NASDAQ", lastSale: 234.57)
         |""".stripMargin,
    isExperimental = true
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}