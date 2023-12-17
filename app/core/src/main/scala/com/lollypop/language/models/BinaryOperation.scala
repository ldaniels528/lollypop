package com.lollypop.language.models

/**
 * Represents a binary operator expression
 */
trait BinaryOperation extends Operation {
  def a: Expression

  def b: Expression

  override def toSQL: String = s"${a.wrapSQL} $operator ${b.wrapSQL}"
}

object BinaryOperation {
  def unapply(o: BinaryOperation): Option[(Expression, Expression)] = Some((o.a, o.b))
}
