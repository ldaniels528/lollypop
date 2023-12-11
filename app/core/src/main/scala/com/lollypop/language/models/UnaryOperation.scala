package com.lollypop.language.models

/**
 * Represents an unary operator expression
 */
trait UnaryOperation extends Operation {
  def a: Expression

  override def toSQL: String = a match {
    case i: IdentifierRef => s"$operator${i.toSQL}"
    case e => s"$operator${e.wrapSQL(required = true)}"
  }
}

object UnaryOperation {
  def unapply(o: UnaryOperation): Option[Expression] = Some(o.a)
}

