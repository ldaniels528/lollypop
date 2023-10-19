package com.qwery.language.models

/**
 * Represents an identifier (e.g. field or variable)
 */
trait IdentifierRef extends NamedExpression {

  override def toSQL: String = if (name.forall(c => c.isLetterOrDigit || c == '_')) name else s"`$name`"

}

object IdentifierRef {

  def unapply(identifier: IdentifierRef): Option[String] = Option(identifier.name)

}
