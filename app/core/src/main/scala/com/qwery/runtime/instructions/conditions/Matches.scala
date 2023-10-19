package com.qwery.runtime.instructions.conditions

import com.qwery.language.HelpDoc.{CATEGORY_PATTERN_MATCHING, PARADIGM_DECLARATIVE}
import com.qwery.language.models.Expression
import com.qwery.language.models.Expression.implicits.LifestyleExpressionsAny
import com.qwery.language.{ExpressionToConditionPostParser, HelpDoc, SQLCompiler, TokenStream}
import com.qwery.runtime.devices.QMap
import com.qwery.runtime.instructions.conditions.Matches.{__name, isMatch}
import com.qwery.runtime.instructions.functions.{AnonymousFunction, AnonymousNamedFunction}
import com.qwery.runtime.{QweryVM, Scope}
import com.qwery.util.JVMSupport._

/**
 * Advanced Pattern Matching
 * @param source  the source value
 * @param pattern the pattern to match
 * @example {{{
 *   val response = { id: 5678, symbol: "DOG", exchange: "NYSE", "lastSale": 90.67 }
 *   response matches { id: isNumber, symbol: isString, exchange: isString, lastSale: isNumber }
 * }}}
 */
case class Matches(source: Expression, pattern: Expression) extends RuntimeCondition {
  override def isTrue(implicit scope: Scope): Boolean = {
    isMatch(src = QweryVM.execute(scope, source)._3, pattern = QweryVM.execute(scope, pattern)._3)
  }

  override def toSQL: String = Seq(source.toSQL, __name, pattern.toSQL).mkString(" ")
}

object Matches extends ExpressionToConditionPostParser {
  private val __name = "matches"

  override def parseConditionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[Matches] = {
    if (ts.nextIf(__name)) compiler.nextExpression(ts).map(Matches(host, _)) else None
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = __name,
    category = CATEGORY_PATTERN_MATCHING,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = s"`value` ${__name} `expression`",
    description = "determines whether the `value` matches the `expression`",
    example =
      """|response = [{ id: 5678, symbol: "DOG", exchange: "NYSE", "lastSale": 90.67 }]
         |isNumber = x => x.isNumber()
         |isString = x => x.isString()
         |response matches [{ id: isNumber, symbol: isString, exchange: isString, lastSale: isNumber }]
         |""".stripMargin
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is __name

  def isMatch(src: Any, pattern: Any)(implicit scope: Scope): Boolean = {
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