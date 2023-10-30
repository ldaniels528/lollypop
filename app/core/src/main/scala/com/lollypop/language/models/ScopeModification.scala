package com.lollypop.language.models

/**
 * Represents a scope-modifying instruction
 */
trait ScopeModification extends Instruction

/**
 * Represents a modification expression
 */
trait ModificationExpression extends ScopeModification with Expression {

  def ref: Expression

  def expression: Expression

}

/**
 * Modification Instruction
 */
object ModificationExpression {
  def unapply(op: ModificationExpression): Option[(Expression, Expression)] = Option((op.ref, op.expression))

}