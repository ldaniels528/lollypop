package com.qwery.language.models

import com.qwery.language.{ExpressionParser, HelpDoc, SQLCompiler, TokenStream}

/**
 * Represents the selection of all fields
 * @author lawrence.daniels@gmail.com
 */
case object AllFields extends FieldRef with ExpressionParser {
  override val name: String = "*"

  override def help: List[HelpDoc] = Nil

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Expression] = {
    if (ts.nextIf("*")) Some(AllFields) else None
  }
  
  override def toSQL: String = name

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "*"

}
