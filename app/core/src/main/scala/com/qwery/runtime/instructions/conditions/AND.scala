package com.qwery.runtime.instructions.conditions

import com.qwery.language.models.Condition
import com.qwery.language.{ConditionToConditionPostParser, HelpDoc, SQLCompiler, TokenStream}
import com.qwery.runtime.Scope

/**
 * SQL: `condition` and `condition`
 * @param a the left-side [[Condition condition]]
 * @param b the right-side [[Condition condition]]
 */
case class AND(a: Condition, b: Condition) extends RuntimeCondition {

  override def isTrue(implicit scope: Scope): Boolean = RuntimeCondition.isTrue(a) && RuntimeCondition.isTrue(b)

  override def toSQL: String = s"${a.wrapSQL} and ${b.wrapSQL}"

}

object AND extends ConditionToConditionPostParser {

  override def help: List[HelpDoc] = Nil

  override def parseConditionChain(ts: TokenStream, host: Condition)(implicit compiler: SQLCompiler): Option[Condition] = {
    if (ts nextIf "and") compiler.nextCondition(ts).map(AND(host, _)) else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "and"

}
