package com.lollypop.runtime.instructions.conditions

import com.lollypop.language.models.Condition
import com.lollypop.language.{ExpressionParser, HelpDoc, SQLCompiler, TokenStream}
import com.lollypop.runtime.Scope

/**
 * SQL: not `condition`
 * @param condition the [[Condition condition]] to evaluate
 */
case class Not(condition: Condition) extends RuntimeCondition {

  override def isTrue(implicit scope: Scope): Boolean = !RuntimeCondition.isTrue(condition)

  override def toSQL: String = s"not ${condition.wrapSQL}"

}

object Not extends ExpressionParser {
  private val __name = "not"

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Not] = {
    if (ts.nextIf(__name)) compiler.nextCondition(ts).map(Not.apply) else None
  }

  override def help: List[HelpDoc] = Nil

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is __name

}