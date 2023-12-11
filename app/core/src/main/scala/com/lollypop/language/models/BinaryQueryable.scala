package com.lollypop.language.models

/**
 * Represents a binary query operation
 */
trait BinaryQueryable extends Queryable {

  def a: Queryable

  def b: Expression

}

object BinaryQueryable {
  def unapply(q: Queryable): Option[(Queryable, Expression)] = {
    q match {
      case bq: BinaryQueryable => Some(bq.a -> bq.b)
      case _ => None
    }
  }

}