package com.qwery.runtime.instructions.conditions

import com.qwery.language.HelpDoc.{CATEGORY_DATAFRAME, PARADIGM_IMPERATIVE}
import com.qwery.language.models.{Condition, Expression}
import com.qwery.language.{ExpressionToConditionPostParser, HelpDoc, SQLCompiler, TokenStream}

object Unlike extends ExpressionToConditionPostParser {
  private val __name = "unlike"

  override def parseConditionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[Condition] = {
    if (ts.nextIf(__name)) compiler.nextExpression(ts).map(b => Not(Like(host, b))) else None
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = __name,
    category = CATEGORY_DATAFRAME,
    paradigm = PARADIGM_IMPERATIVE,
    syntax = s"`value` ${__name} `expression`",
    description = "determines whether the `value` does not match the `expression`",
    example = """"Chris" unlike "h%s""""
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is __name

}