package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.models.Expression
import com.lollypop.runtime.LollypopNative
import com.lollypop.runtime.datatypes.StringType

trait StringExpression extends Expression with LollypopNative {

  override def returnType: StringType = StringType

}
