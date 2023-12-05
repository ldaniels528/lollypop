package com.lollypop.runtime.instructions.functions

import com.lollypop.language.models._
import com.lollypop.runtime.Scope
import com.lollypop.runtime.instructions.RuntimeInstruction
import lollypop.io.IOCost

/**
 * Represents a named function
 * @param name   the name of the function
 * @param params the optional collection of function [[ParameterLike parameters]]
 * @param code   the function [[Instruction code]] to execute when the function is invoked.
 * @example def factorial(n: Int): Int := iff(n <= 1, 1, n * factorial(n - 1))
 */
case class NamedFunction(name: String, params: Seq[ParameterLike], code: Instruction, returnType_? : Option[ColumnType])
  extends TypicalFunction with RuntimeInstruction with NamedExpression {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    (scope.withVariable(name, code = this, isReadOnly = true), IOCost.empty, this)
  }

  override def toSQL: String = (name :: params.map(_.toSQL).mkString("(", ", ", ")") ::
    returnType_?.toList.map(r => ": " + r.toSQL) ::: " := " :: code.toSQL :: Nil).mkString

}