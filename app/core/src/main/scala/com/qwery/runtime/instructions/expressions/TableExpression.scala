package com.qwery.runtime.instructions.expressions

import com.qwery.runtime.QweryNative
import com.qwery.runtime.datatypes.TableType

/**
 * Represents an object that can rendered as a table and whose structure is known before execution.
 */
trait TableExpression extends QweryNative {

  def returnType: TableType

}