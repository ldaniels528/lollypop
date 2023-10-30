package com.lollypop.runtime.datatypes

import com.lollypop.language.models.Expression
import com.lollypop.runtime.instructions.expressions.NamedFunctionCall

/**
 * Implemented by classes that can be instantiate
 */
trait ConstructorSupport[T] {

  def construct(args: Seq[Any]): T

}

trait ConstructorSupportCompanion { dataType: DataType =>

  def apply(args: Expression*): NamedFunctionCall = NamedFunctionCall(dataType.name, args.toList)

}