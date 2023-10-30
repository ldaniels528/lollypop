package com.lollypop.runtime

import com.lollypop.runtime.datatypes.DataType
import com.lollypop.runtime.errors.VariableIsReadOnlyError

/**
 * Represents a variable
 * @param name         the name of the variable
 * @param _type        the [[DataType data type]]
 * @param initialValue the initial value
 * @param isReadOnly   indicates whether the variable is immutable (read-only)
 */
case class Variable(name: String, _type: DataType, initialValue: Any = null, isReadOnly: Boolean = false)
  extends ValueReference {
  private var myValue: Any = _type.convert(initialValue)

  override def `type`(implicit scope: Scope): DataType = _type

  def value(implicit scope: Scope): Any = myValue

  def value_=(newValue: Any)(implicit scope: Scope): Unit = {
    if (isReadOnly) throw new VariableIsReadOnlyError(name) else myValue = `type`.convert(newValue)
  }
}