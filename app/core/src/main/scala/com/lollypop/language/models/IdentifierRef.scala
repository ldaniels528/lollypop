package com.lollypop.language.models

import com.lollypop.util.OptionHelper.OptionEnrichment

/**
 * Represents an identifier (e.g. field or variable)
 */
trait IdentifierRef extends NamedExpression {

  override def toSQL: String = if (name.forall(c => c.isLetterOrDigit || c == '_')) name else s"`$name`"

}

object IdentifierRef {

  def unapply(identifier: IdentifierRef): Option[String] = identifier.alias ?? Some(identifier.name)

}
