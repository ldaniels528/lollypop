package com.lollypop.runtime.instructions.conditions

import com.lollypop.language.models.Condition
import com.lollypop.language.{ConditionToConditionPostParser, HelpDoc, SQLCompiler, TokenStream}
import com.lollypop.runtime.Scope
import lollypop.io.IOCost

/**
 * SQL: `condition` OR `condition`
 * @param a the left-side [[Condition condition]]
 * @param b the right-side [[Condition condition]]
 */
case class OR(a: Condition, b: Condition) extends RuntimeCondition {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Boolean) = {
    val result = RuntimeCondition.isTrue(a) || RuntimeCondition.isTrue(b)
    (scope, IOCost.empty, result)
  }

  override def toSQL: String = s"${a.wrapSQL} or ${b.wrapSQL}"

}

object OR extends ConditionToConditionPostParser {

  override def help: List[HelpDoc] = Nil

  override def parseConditionChain(ts: TokenStream, host: Condition)(implicit compiler: SQLCompiler): Option[Condition] = {
    if (ts nextIf "or") compiler.nextCondition(ts).map(OR(host, _)) else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "or"

}