package com.lollypop.runtime.instructions.expressions

import com.lollypop.runtime.LollypopNative
import com.lollypop.runtime.datatypes.TableType

/**
 * Represents an object that can rendered as a table and whose structure is known before execution.
 */
trait TableExpression extends LollypopNative {

  def returnType: TableType

}