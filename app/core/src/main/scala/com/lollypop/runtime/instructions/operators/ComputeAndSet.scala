package com.lollypop.runtime.instructions.operators

import com.lollypop.implicits.MagicImplicits
import com.lollypop.language.models.{Expression, ModificationExpression, BinaryOperation}
import com.lollypop.runtime.Scope
import com.lollypop.runtime.instructions.invocables.RuntimeInvokable
import lollypop.io.IOCost

/**
 * Evaluates an operation and updates a variable with the result.
 * @param operation the [[BinaryOperation operation]]
 */
case class ComputeAndSet(operation: BinaryOperation) extends RuntimeInvokable with ModificationExpression {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    scope.resolveReferenceName(ref) ~> { name => (scope.setVariable(name, operation), IOCost.empty, null) }
  }

  override def ref: Expression = operation.a

  override def expression: Expression = operation.b

  override def toSQL: String = s"${ref.toSQL} ${operation.operator}= ${expression.toSQL}"
}

object ComputeAndSet {

  final implicit class ComputeAndSetSugar(val operation: BinaryOperation) extends AnyVal {
   def doAndSet: ComputeAndSet = ComputeAndSet(operation)
  }

}