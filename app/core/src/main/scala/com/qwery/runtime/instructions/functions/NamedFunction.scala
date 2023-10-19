package com.qwery.runtime.instructions.functions

import com.qwery.language.models._
import com.qwery.runtime.Scope
import com.qwery.runtime.instructions.RuntimeInstruction
import qwery.io.IOCost

/**
 * Represents a named function
 * @param name   the name of the function
 * @param params the collection of function [[ParameterLike parameters]]
 * @param code   the function [[Instruction code]]
 * @example function factorial(n: int): int = n <= 1 ? 1 : n * factorial(n - 1)
 */
case class NamedFunction(name: String, params: Seq[ParameterLike], code: Instruction, returnType_? : Option[ColumnType])
  extends TypicalFunction with RuntimeInstruction with NamedExpression {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    (scope.withVariable(name, code = this, isReadOnly = true), IOCost.empty, this)
  }

  override def toSQL: String = (name :: params.map(_.toSQL).mkString("(", ", ", ")") ::
    returnType_?.toList.map(r => ": " + r.toSQL) ::: " := " :: code.toSQL :: Nil).mkString

}