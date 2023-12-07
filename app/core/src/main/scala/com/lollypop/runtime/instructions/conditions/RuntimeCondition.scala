package com.lollypop.runtime.instructions.conditions

import com.lollypop.language.dieUnsupportedEntity
import com.lollypop.language.models._
import com.lollypop.runtime.datatypes.BooleanType
import com.lollypop.runtime.instructions.expressions.RuntimeExpression
import com.lollypop.runtime.instructions.queryables.RuntimeQueryable
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost

/**
 * Runtime Condition
 */
trait RuntimeCondition extends RuntimeExpression with Condition {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Boolean)

  def isTrue(implicit scope: Scope): Boolean = execute()._3

  def isFalse(implicit scope: Scope): Boolean = !isTrue

}

/**
 * Run-time Condition Companion
 */
object RuntimeCondition {

  def isTrue(expression: Expression)(implicit scope: Scope): Boolean = expression match {
    case c: RuntimeCondition => c.isTrue
    case e: RuntimeExpression => BooleanType.convert(e.execute()._3)
    case q: RuntimeQueryable => BooleanType.convert(q.search(scope)._3)
    case u => dieUnsupportedEntity(u, entityName = "condition")
  }

  def isFalse(expression: Expression)(implicit scope: Scope): Boolean = !isTrue(expression)

}