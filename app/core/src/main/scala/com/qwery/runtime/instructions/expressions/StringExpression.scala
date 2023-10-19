package com.qwery.runtime.instructions.expressions

import com.qwery.language.models.Expression
import com.qwery.runtime.QweryNative
import com.qwery.runtime.datatypes.StringType

trait StringExpression extends Expression with QweryNative {

  override def returnType: StringType = StringType

}
