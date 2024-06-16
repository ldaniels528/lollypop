package com.lollypop.runtime.instructions.functions

import com.lollypop.language.models.{Expression, IdentifierRef}

/**
 * Represents a decomposition function to be used by the decompose table expression.
 */
trait DecompositionFunction extends Expression {

  def columnName: Expression

  def getColumnName: String = {
    columnName match {
      case IdentifierRef(name) => name
      case z => dieIllegalType(z)
    }
  }

}
