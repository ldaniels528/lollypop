package com.lollypop.runtime.instructions.conditions

import com.lollypop.language.models.Condition
import com.lollypop.language.{ExpressionParser, HelpDoc, SQLCompiler, TokenStream}
import com.lollypop.runtime.Scope
import lollypop.io.IOCost

/**
 * SQL: not `condition`
 * @param condition the [[Condition condition]] to evaluate
 */
case class Not(condition: Condition) extends RuntimeCondition {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Boolean) = {
    (scope, IOCost.empty, !RuntimeCondition.isTrue(condition))
  }

  override def toSQL: String = s"not ${condition.wrapSQL}"

}

object Not extends ExpressionParser {
  private val keyword = "not"

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Not] = {
    if (ts.nextIf(keyword)) compiler.nextCondition(ts).map(Not.apply) else None
  }

  override def help: List[HelpDoc] = Nil

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}