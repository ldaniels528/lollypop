package com.lollypop.language.models

import com.lollypop.runtime.LollypopNative
import com.lollypop.runtime.datatypes.BooleanType

/**
 * Base class for all conditional expressions
 * @author lawrence.daniels@gmail.com
 */
trait Condition extends Expression with LollypopNative {

  override def returnType: BooleanType = BooleanType

}