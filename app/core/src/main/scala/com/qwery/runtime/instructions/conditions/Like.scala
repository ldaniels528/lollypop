package com.qwery.runtime.instructions.conditions

import com.qwery.language.HelpDoc.{CATEGORY_DATAFRAME, PARADIGM_IMPERATIVE}
import com.qwery.language.models.Expression.implicits.LifestyleExpressionsAny
import com.qwery.language.models.{Expression, LambdaFunction}
import com.qwery.language.{ExpressionToConditionPostParser, HelpDoc, SQLCompiler, TokenStream}
import com.qwery.runtime.devices.QMap
import com.qwery.runtime.instructions.conditions.Like.__name
import com.qwery.runtime.instructions.functions.{AnonymousFunction, AnonymousNamedFunction}
import com.qwery.runtime.{QweryVM, Scope}
import com.qwery.util.JVMSupport.NormalizeAny

/**
 * Like: Advanced Pattern Matching Operator
 * @param a the [[Expression expression]] to evaluate
 * @param b the pattern [[Expression expression]]
 * @example {{{
 *   "Hello World" like "H% W%"
 * }}}
 * @example {{{
 *   isNumeric = (o: object) => o.isNumber()
 *   5678 like isNumeric
 * }}}
 * @example {{{
 *   response = { id: 5678, symbol: "DOG", exchange: "NYSE", lastSale: 90.67 }
 *   isExchange = (s: String) => s in ['NYSE', 'AMEX', 'NASDAQ', 'OTCBB']
 *   isNumber = (o: Any) => o.isNumber()
 *   isString = (o: Any) => o.isString()
 *   response like { id: isNumber, symbol: isString, exchange: isExchange, lastSale: isNumber }
 * }}}
 */
case class Like(a: Expression, b: Expression) extends RuntimeCondition {
  override def isTrue(implicit scope: Scope): Boolean = {
    val (_, _, subject) = QweryVM.execute(scope, a)
    val (_, _, target) = QweryVM.execute(scope, b)
    target match {
      // "Hello World" like "H% W%"
      case pattern: String =>
        subject match {
          case text: String => text.matches(pattern.replace("%", ".*"))
          case z => b.dieIllegalType(z)
        }
      // 5678 like isNumeric
      case function: LambdaFunction => isMatch(src = subject, pattern = function)
      // response like { id: isNumber, symbol: isString, exchange: isExchange, lastSale: isNumber }
      case pattern => isMatch(src = subject, pattern = pattern)
    }
  }

  override def toSQL: String = s"${a.toSQL} ${__name} ${b.toSQL}"

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

object Like extends ExpressionToConditionPostParser {
  private val __name = "like"
  private val templateCard = "%e:source like %e:target"

  override def parseConditionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[Like] = {
    if (ts.nextIf(__name)) compiler.nextExpression(ts).map(Like(host, _)) else None
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = __name,
    category = CATEGORY_DATAFRAME,
    paradigm = PARADIGM_IMPERATIVE,
    syntax = templateCard,
    description = "determines whether the `value` matches the `expression`",
    example =
      """|"Hello World" like "H% W%"
         |""".stripMargin
  ), HelpDoc(
    name = __name,
    category = CATEGORY_DATAFRAME,
    paradigm = PARADIGM_IMPERATIVE,
    syntax = templateCard,
    description = "determines whether the `value` matches the `expression`",
    example =
      """|isNumeric = (o: object) => o.isNumber()
         |5678 like isNumeric
         |""".stripMargin
  ), HelpDoc(
    name = __name,
    category = CATEGORY_DATAFRAME,
    paradigm = PARADIGM_IMPERATIVE,
    syntax = templateCard,
    description = "determines whether the `value` matches the `expression`",
    example =
      """|response = { id: 5678, symbol: "DOG", exchange: "NYSE", lastSale: 90.67 }
         |isExchange = (s: String) => s in ['NYSE', 'AMEX', 'NASDAQ', 'OTCBB']
         |isNumber = (o: Any) => o.isNumber()
         |isString = (o: Any) => o.isString()
         |response like { id: isNumber, symbol: isString, exchange: isExchange, lastSale: isNumber }
         |""".stripMargin
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is __name

}