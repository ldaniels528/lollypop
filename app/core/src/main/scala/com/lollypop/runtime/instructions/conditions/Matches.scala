package com.lollypop.runtime.instructions.conditions

import com.lollypop.language.HelpDoc.{CATEGORY_FILTER_MATCH_OPS, PARADIGM_DECLARATIVE, PARADIGM_FUNCTIONAL}
import com.lollypop.language.models.Expression.implicits.LifestyleExpressionsAny
import com.lollypop.language.models.{Expression, LambdaFunction, Literal}
import com.lollypop.language.{ExpressionToConditionPostParser, HelpDoc, SQLCompiler, TokenStream}
import com.lollypop.runtime.LollypopVM.implicits.InstructionExtensions
import com.lollypop.runtime.Scope
import com.lollypop.runtime.devices.QMap
import com.lollypop.runtime.instructions.conditions.Matches.keyword
import com.lollypop.runtime.instructions.expressions.{NamedFunctionCall, New}
import com.lollypop.runtime.instructions.functions.AnonymousNamedFunction
import com.lollypop.util.JVMSupport.NormalizeAny
import lollypop.io.IOCost

/**
 * Matches: Advanced Pattern Matching Operator
 * @param expression the [[Expression expression]] to evaluate
 * @param pattern    the pattern [[Expression expression]]
 * @example {{{
 *   "Hello World 123" matches "H(.*) W(.*) \d+"
 * }}}
 * @example {{{
 *   isEven = x => (x.isNumber() is true) and ((x % 2) is 0)
 *   5678 matches isEven
 * }}}
 * @example {{{
 *   response = { id: 5678, symbol: "DOG", exchange: "NYSE", lastSale: 90.67 }
 *   response matches {
 *      id: _ => true,
 *      symbol: "DOG",
 *      exchange: "NYSE",
 *      lastSale: 90.67
 *   }
 * }}}
 * @example {{{
 *   class Stock(symbol: String, exchange: String, lastSale: Double)
 *   stock = new Stock(symbol: "AAPL", exchange: "NASDAQ", lastSale: 234.57)
 *   stock matches Stock(
 *      symbol: x => (x.isString() is true) and
 *                   (x.length() between 1 and 6) and
 *                   (x.forall(c => Character.isAlphabetic(c)) is true),
 *      exchange: x => x in ['NYSE', 'AMEX', 'NASDAQ', 'OTCBB'],
 *      lastSale: n => n >= 200.00
 *   )
 * }}}
 */
case class Matches(expression: Expression, pattern: Expression) extends RuntimeCondition {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Boolean) = {
    val (scopeA, costA, aValue) = expression.execute(scope)
    val result = pattern match {
      // "Hello World 123" matches "H(.*) W(.*) \d+"
      case Literal(textPattern: String) =>
        aValue match {
          case text: String => text.matches(textPattern)
          case z => pattern.dieIllegalType(z)
        }
      // 5678 matches isEven
      case fx: LambdaFunction => isMatch(src = aValue, pattern = fx)
      // stock matches Stock(symbol: _ => true, exchange: _ => true, lastSale: n => n < 1)
      case fx: NamedFunctionCall => isMatchProduct(instA = aValue, pattern = fx)
      // response matches { symbol: _ => true, exchange: _ => true, lastSale: n => n betwixt 0 and 0.2 }
      case pattern =>
        val (_, _, objectPattern) = pattern.execute(scopeA)
        isMatch(src = aValue, pattern = objectPattern)
    }
    (scope, costA, result)
  }

  override def toSQL: String = Seq(expression.toSQL, keyword, pattern.toSQL).mkString(" ")

  private def isMatch(src: Any, pattern: Any)(implicit scope: Scope): Boolean = {
    (src.normalizeArrays, pattern.normalizeArrays) match {
      case (a: QMap[String, _], b: QMap[String, _]) => isMatchMap(a, b)
      case (a: QMap[String, _], b: Seq[_]) => isMatchSeq(a.toSeq, b)
      case (a: Seq[_], b: QMap[String, _]) => isMatchSeq(a, b.toSeq)
      case (a: Seq[_], b: Seq[_]) => isMatchSeq(a, b)
      case (a, b: LambdaFunction) => b.call(List(a.v)).execute(scope)._3 == true
      case (a, b) => a == b
    }
  }

  private def isMatchMap(src: QMap[String, Any], pattern: QMap[String, Any])(implicit scope: Scope): Boolean = {
    if (src.keys != pattern.keys) false
    else {
      val results = pattern map {
        case (name: String, af: LambdaFunction) =>
          src.get(name).exists { value => af.call(List(value.v)).execute(scope)._3 == true }
        case (name: String, af: AnonymousNamedFunction) =>
          src.get(name).exists { value => af.call(List(value.v)).execute()._3 == true }
        case (name: String, value) => src.get(name).exists(isMatch(_, value))
      }
      results.forall(_ == true)
    }
  }

  private def isMatchProduct(instA: Any, pattern: NamedFunctionCall)(implicit scope: Scope): Boolean = {
    val (_, _, instB) = New(typeName = pattern.name, pattern.args).execute(scope)
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
      """|"Hello World 123" matches "H(.*) W(.*) \d+"
         |""".stripMargin
  ), HelpDoc(
    name = keyword,
    category = CATEGORY_FILTER_MATCH_OPS,
    paradigm = PARADIGM_FUNCTIONAL,
    syntax = templateCard,
    description = "determines whether the `value` matches the `expression`",
    example =
      """|isEven = x => (x.isNumber() is true) and ((x % 2) is 0)
         |5678 matches isEven
         |""".stripMargin
  ), HelpDoc(
    name = keyword,
    category = CATEGORY_FILTER_MATCH_OPS,
    paradigm = PARADIGM_FUNCTIONAL,
    syntax = templateCard,
    description = "determines whether the `value` matches the `expression`",
    example =
      """|response = { id: 5678, symbol: "DOG", exchange: "NYSE", lastSale: 90.67 }
         |response matches {
         |   id: _ => true
         |   symbol: "DOG"
         |   exchange: "NYSE"
         |   lastSale: 90.67
         |}
         |""".stripMargin
  ), HelpDoc(
    name = keyword,
    category = CATEGORY_FILTER_MATCH_OPS,
    paradigm = PARADIGM_FUNCTIONAL,
    syntax = templateCard,
    description = "determines whether the `value` matches the `expression`",
    example =
      """|class Stock(symbol: String, exchange: String, lastSale: Double)
         |stock = new Stock(symbol: "ATX", exchange: "NASDAQ", lastSale: 234.57)
         |stock matches Stock(
         |    symbol: x => (x.isString() is true) and
         |                 (x.length() between 1 and 6) and
         |                 (x.forall(c => Character.isAlphabetic(c)) is true),
         |    exchange: x => x in ['NYSE', 'AMEX', 'NASDAQ', 'OTCBB'],
         |    lastSale: n => n >= 0 and n < 500
         |)
         |""".stripMargin,
    isExperimental = true
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}