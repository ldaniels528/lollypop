package com.qwery.runtime.instructions.expressions

import com.qwery.language.HelpDoc.{CATEGORY_SYSTEMS, PARADIGM_IMPERATIVE}
import com.qwery.language.models.Expression
import com.qwery.language.{ExpressionParser, HelpDoc, SQLCompiler, TokenStream}
import com.qwery.runtime.Scope

import scala.util.Properties

/**
 * whoami - Returns the name of the current user
 */
class Whoami extends RuntimeExpression {
  override def evaluate()(implicit scope: Scope): Any = Properties.userName

  override def equals(obj: Any): Boolean = obj.isInstanceOf[Whoami]

  override def toSQL = "whoami"
}

object Whoami extends ExpressionParser {
  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Expression] = {
    if (ts.nextIf("whoami")) Some(new Whoami()) else None
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "whoami",
    category = CATEGORY_SYSTEMS,
    paradigm = PARADIGM_IMPERATIVE,
    syntax = "whoami",
    description = "Returns the name of the current user",
    example = "whoami"
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "whoami"
}