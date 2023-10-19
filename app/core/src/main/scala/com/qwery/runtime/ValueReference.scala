package com.qwery.runtime

import com.qwery.runtime.datatypes.DataType

/**
 * Represents an identifier-referenced value (e.g. a variable)
 */
trait ValueReference {

  def name: String

  def `type`(implicit scope: Scope): DataType

  def value(implicit scope: Scope): Any

  def value_=(newValue: Any)(implicit scope: Scope): Unit

}