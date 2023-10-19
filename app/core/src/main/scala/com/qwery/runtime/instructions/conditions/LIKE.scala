package com.qwery.runtime.instructions.conditions

import com.qwery.language.HelpDoc.{CATEGORY_DATAFRAME, PARADIGM_IMPERATIVE}
import com.qwery.language.models.Expression
import com.qwery.language.{ExpressionToConditionPostParser, HelpDoc, SQLCompiler, TokenStream}
import com.qwery.runtime.Scope
import com.qwery.runtime.instructions.conditions.Like.__name
import com.qwery.runtime.instructions.expressions.RuntimeExpression.RichExpression

/**
 * SQL: `expression` like `pattern`
 * @param a the [[Expression expression]] to evaluate
 * @param b the pattern [[Expression expression]]
 */
case class Like(a: Expression, b: Expression) extends RuntimeCondition {
  override def isTrue(implicit scope: Scope): Boolean = {
    (for {text <- a.asString; pattern <- b.asString} yield text.matches(pattern.replace("%", ".*"))).contains(true)
  }

  override def toSQL: String = s"${a.toSQL} ${__name} ${b.toSQL}"
}

object Like extends ExpressionToConditionPostParser {
  private val __name = "like"

  override def parseConditionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[Like] = {
    if (ts.nextIf(__name)) compiler.nextExpression(ts).map(Like(host, _)) else None
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = __name,
    category = CATEGORY_DATAFRAME,
    paradigm = PARADIGM_IMPERATIVE,
    syntax = s"`value` ${__name} `expression`",
    description = "determines whether the `value` matches the `expression`",
    example = s""""Chris" ${__name} "Ch%s""""
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is __name

}